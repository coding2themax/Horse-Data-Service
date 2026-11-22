package com.coding2themax.horse.order.application;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;

import com.coding2themax.horse.order.domain.OrderStatus;
import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.PricedLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ApproveOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.CancelOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApprovalFailed;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApproved;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderCancelled;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.OutboxStore;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Applies order saga commands inside the inbox transaction. Decisions use the
 * current order status, so retried commands with new message ids are safe.
 */
@Service
public class OrderCommandService {

  private static final String LOCK_STATUS = "SELECT status FROM orders WHERE order_id = :orderId FOR UPDATE";
  private static final String FIND_QUANTITIES = "SELECT product_id, quantity FROM order_items WHERE order_id = :orderId";
  private static final String SET_ITEM_PRICE = """
      UPDATE order_items SET unit_price = :unitPrice WHERE order_id = :orderId AND product_id = :productId
      """;
  private static final String APPROVE = """
      UPDATE orders SET status = 'APPROVED', status_reason = NULL, total_price = :total, updated_at = now()
      WHERE order_id = :orderId
      """;
  private static final String CANCEL = """
      UPDATE orders SET status = 'CANCELLED', status_reason = :reason, updated_at = now()
      WHERE order_id = :orderId
      """;

  private final DatabaseClient database;
  private final OutboxStore outbox;

  public OrderCommandService(DatabaseClient database, OutboxStore outbox) {
    this.database = database;
    this.outbox = outbox;
  }

  public Mono<Void> handle(MessageEnvelope envelope, SagaMessage message) {
    return switch (message) {
      case ApproveOrder command -> approve(envelope.sagaId(), command);
      case CancelOrder command -> cancel(envelope.sagaId(), command);
      default -> Mono.error(new IllegalArgumentException("Unsupported order command: " + envelope.type()));
    };
  }

  private Mono<Void> approve(UUID sagaId, ApproveOrder command) {
    return lockStatus(command.orderId()).flatMap(status -> {
      if (status.isEmpty()) {
        return approvalFailed(sagaId, command, "Order not found");
      }
      return switch (status.get()) {
        case PLACED -> approvePlaced(sagaId, command);
        case APPROVED -> reply(sagaId, new OrderApproved(command.orderId()));
        default -> approvalFailed(sagaId, command, "Order is " + status.get());
      };
    });
  }

  private Mono<Void> approvePlaced(UUID sagaId, ApproveOrder command) {
    Map<Integer, Integer> reserved = command.lines().stream()
        .collect(Collectors.toMap(PricedLine::productId, PricedLine::quantity, Math::addExact));
    return database.sql(FIND_QUANTITIES)
        .bind("orderId", command.orderId())
        .map(row -> Map.entry(row.get("product_id", Integer.class), row.get("quantity", Integer.class)))
        .all()
        .collectMap(Map.Entry::getKey, Map.Entry::getValue)
        .flatMap(ordered -> ordered.equals(reserved) && reserved.size() == command.lines().size()
            ? applyApproval(sagaId, command)
            : approvalFailed(sagaId, command, "Reserved lines do not match the order"));
  }

  private Mono<Void> applyApproval(UUID sagaId, ApproveOrder command) {
    BigDecimal total = command.lines().stream()
        .map(line -> line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
    return Flux.fromIterable(command.lines())
        .concatMap(line -> database.sql(SET_ITEM_PRICE)
            .bind("unitPrice", line.unitPrice())
            .bind("orderId", command.orderId())
            .bind("productId", line.productId())
            .fetch()
            .rowsUpdated())
        .then(database.sql(APPROVE)
            .bind("total", total)
            .bind("orderId", command.orderId())
            .fetch()
            .rowsUpdated())
        .then(reply(sagaId, new OrderApproved(command.orderId())));
  }

  private Mono<Void> cancel(UUID sagaId, CancelOrder command) {
    return lockStatus(command.orderId()).flatMap(status -> {
      if (status.isEmpty()) {
        return Mono.error(new IllegalStateException("Cannot cancel unknown order " + command.orderId()));
      }
      return switch (status.get()) {
        // APPROVED is cancellable: the saga may compensate after timing out on an approval.
        case PLACED, APPROVED -> applyCancel(sagaId, command);
        case CANCELLED -> reply(sagaId, new OrderCancelled(command.orderId()));
        case DELIVERED -> Mono.error(new IllegalStateException("Cannot cancel delivered order " + command.orderId()));
      };
    });
  }

  private Mono<Void> applyCancel(UUID sagaId, CancelOrder command) {
    String reason = command.reason() == null ? "Order saga cancelled" : command.reason();
    return database.sql(CANCEL)
        .bind("reason", reason)
        .bind("orderId", command.orderId())
        .fetch()
        .rowsUpdated()
        .then(reply(sagaId, new OrderCancelled(command.orderId())));
  }

  private Mono<Optional<OrderStatus>> lockStatus(UUID orderId) {
    return database.sql(LOCK_STATUS)
        .bind("orderId", orderId)
        .map(row -> Optional.of(OrderStatus.valueOf(row.get("status", String.class))))
        .one()
        .defaultIfEmpty(Optional.empty());
  }

  private Mono<Void> approvalFailed(UUID sagaId, ApproveOrder command, String reason) {
    return reply(sagaId, new OrderApprovalFailed(command.orderId(), reason));
  }

  private Mono<Void> reply(UUID sagaId, SagaMessage message) {
    return outbox.append(SagaTopics.ORDER_REPLIES, sagaId, message).then();
  }
}
