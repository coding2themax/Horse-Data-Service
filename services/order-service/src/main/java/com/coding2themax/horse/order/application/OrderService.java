package com.coding2themax.horse.order.application;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.order.api.dto.CreateOrderRequest;
import com.coding2themax.horse.order.api.dto.OrderItemResponse;
import com.coding2themax.horse.order.api.dto.OrderLineRequest;
import com.coding2themax.horse.order.api.dto.OrderResponse;
import com.coding2themax.horse.order.domain.OrderStatus;
import com.coding2themax.horse.order.infrastructure.CustomerRepository;
import com.coding2themax.horse.saga.contracts.OrderLine;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderPlaced;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.OutboxStore;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class OrderService {

  private static final String INSERT_ORDER = """
      INSERT INTO orders (order_id, saga_id, customer_id, status, idempotency_key, request_fingerprint)
      VALUES (:orderId, :sagaId, :customerId, 'PLACED', :key, :fingerprint)
      ON CONFLICT (idempotency_key) DO NOTHING
      """;
  private static final String FIND_BY_KEY = """
      SELECT order_id, request_fingerprint FROM orders WHERE idempotency_key = :key
      """;
  private static final String INSERT_ITEM = """
      INSERT INTO order_items (order_id, product_id, quantity) VALUES (:orderId, :productId, :quantity)
      """;
  private static final String FIND_ORDER = """
      SELECT order_id, saga_id, customer_id, status, status_reason, order_date, total_price
      FROM orders WHERE order_id = :orderId
      """;
  private static final String FIND_ITEMS = """
      SELECT product_id, quantity, unit_price FROM order_items WHERE order_id = :orderId ORDER BY product_id
      """;

  private final DatabaseClient database;
  private final CustomerRepository customers;
  private final OutboxStore outbox;
  private final TransactionalOperator transaction;

  public OrderService(DatabaseClient database, CustomerRepository customers, OutboxStore outbox,
      ReactiveTransactionManager transactionManager) {
    this.database = database;
    this.customers = customers;
    this.outbox = outbox;
    this.transaction = TransactionalOperator.create(transactionManager);
  }

  /**
   * Stores the order and the event that starts its saga in one transaction. The
   * unique key makes concurrent retries wait, then return the committed order.
   */
  public Mono<PlacedOrder> place(UUID idempotencyKey, CreateOrderRequest request) {
    UUID orderId = UUID.randomUUID();
    UUID sagaId = UUID.randomUUID();
    return Mono.fromCallable(() -> mergeLines(request.lines()))
        .flatMap(lines -> transaction.transactional(customers.existsById(request.customerId())
            .flatMap(exists -> exists
                ? insertOrReplay(idempotencyKey, orderId, sagaId, request.customerId(), lines)
                : Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "Customer " + request.customerId() + " not found")))))
        .flatMap(storedId -> findById(storedId)
            .map(order -> new PlacedOrder(order, !storedId.equals(orderId))));
  }

  public Mono<OrderResponse> findById(UUID orderId) {
    Mono<List<OrderItemResponse>> items = database.sql(FIND_ITEMS)
        .bind("orderId", orderId)
        .map(row -> new OrderItemResponse(row.get("product_id", Integer.class), row.get("quantity", Integer.class),
            row.get("unit_price", BigDecimal.class)))
        .all()
        .collectList();
    return database.sql(FIND_ORDER)
        .bind("orderId", orderId)
        .map(row -> new OrderResponse(row.get("order_id", UUID.class), row.get("saga_id", UUID.class),
            row.get("customer_id", Integer.class), OrderStatus.valueOf(row.get("status", String.class)),
            row.get("status_reason", String.class), row.get("order_date", OffsetDateTime.class),
            row.get("total_price", BigDecimal.class), List.of()))
        .one()
        .switchIfEmpty(Mono.error(
            () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + orderId + " not found")))
        .zipWith(items, (order, lines) -> new OrderResponse(order.id(), order.sagaId(), order.customerId(),
            order.status(), order.statusReason(), order.orderDate(), order.totalPrice(), lines));
  }

  private Mono<UUID> insertOrReplay(UUID key, UUID orderId, UUID sagaId, int customerId, List<OrderLine> lines) {
    String fingerprint = fingerprint(customerId, lines);
    return database.sql(INSERT_ORDER)
        .bind("orderId", orderId)
        .bind("sagaId", sagaId)
        .bind("customerId", customerId)
        .bind("key", key)
        .bind("fingerprint", fingerprint)
        .fetch()
        .rowsUpdated()
        .flatMap(inserted -> inserted == 1
            ? insertLinesAndEvent(orderId, sagaId, lines).thenReturn(orderId)
            : findReplay(key, fingerprint));
  }

  private Mono<UUID> findReplay(UUID key, String fingerprint) {
    return database.sql(FIND_BY_KEY)
        .bind("key", key)
        .map(row -> Map.entry(row.get("order_id", UUID.class), row.get("request_fingerprint", String.class)))
        .one()
        .flatMap(stored -> stored.getValue().equals(fingerprint)
            ? Mono.just(stored.getKey())
            : Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                "Idempotency key was used with a different request")));
  }

  private Mono<Void> insertLinesAndEvent(UUID orderId, UUID sagaId, List<OrderLine> lines) {
    return Flux.fromIterable(lines)
        .concatMap(line -> database.sql(INSERT_ITEM)
            .bind("orderId", orderId)
            .bind("productId", line.productId())
            .bind("quantity", line.quantity())
            .fetch()
            .rowsUpdated())
        .then(outbox.append(SagaTopics.ORDER_EVENTS, sagaId, new OrderPlaced(orderId, lines)))
        .then();
  }

  /** Lines are already merged and sorted, so equivalent requests match. */
  private static String fingerprint(int customerId, List<OrderLine> lines) {
    return customerId + ":" + lines.stream()
        .map(line -> line.productId() + "x" + line.quantity())
        .collect(Collectors.joining(","));
  }

  private static List<OrderLine> mergeLines(List<OrderLineRequest> lines) {
    Map<Integer, Integer> merged = new TreeMap<>();
    try {
      lines.forEach(line -> merged.merge(line.productId(), line.quantity(), Math::addExact));
    } catch (ArithmeticException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Quantity is too large");
    }
    return merged.entrySet().stream().map(line -> new OrderLine(line.getKey(), line.getValue())).toList();
  }
}
