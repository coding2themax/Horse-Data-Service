package com.coding2themax.horse.stock.application;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.OrderLine;
import com.coding2themax.horse.saga.contracts.PricedLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReleaseStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReserveStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockRejected;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReleased;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReserved;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.OutboxStore;
import com.coding2themax.horse.stock.domain.ReservationStatus;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Applies stock saga commands. Must run inside the inbox transaction, so the
 * stock change, reservation, and reply commit together. Decisions are keyed by
 * saga because a retried command can arrive with a new message id.
 */
@Service
public class StockReservationService {

  private static final String FIND_STATUS = "SELECT status FROM stock_reservations WHERE saga_id = :sagaId FOR UPDATE";
  private static final String LOCK_PRODUCTS = """
      SELECT product_id, stock, price FROM products
      WHERE product_id = ANY(:ids)
      ORDER BY product_id
      FOR UPDATE
      """;
  private static final String INSERT_RESERVATION = """
      INSERT INTO stock_reservations (saga_id, order_id, status) VALUES (:sagaId, :orderId, :status)
      """;
  private static final String INSERT_LINE = """
      INSERT INTO stock_reservation_lines (saga_id, product_id, quantity, unit_price)
      VALUES (:sagaId, :productId, :quantity, :unitPrice)
      """;
  private static final String FIND_LINES = """
      SELECT product_id, quantity, unit_price FROM stock_reservation_lines
      WHERE saga_id = :sagaId
      ORDER BY product_id
      """;
  private static final String TAKE_STOCK = """
      UPDATE products SET stock = stock - :quantity
      WHERE product_id = :productId AND stock >= :quantity
      """;
  private static final String MARK_RELEASED = """
      UPDATE stock_reservations SET status = 'RELEASED', updated_at = now() WHERE saga_id = :sagaId
      """;
  private static final String RESTORE_LINES = """
      UPDATE products p SET stock = p.stock + l.quantity
      FROM stock_reservation_lines l
      WHERE l.saga_id = :sagaId AND p.product_id = l.product_id
      """;
  private static final String LOCK_RESERVED_PRODUCTS = """
      SELECT p.product_id FROM products p
      JOIN stock_reservation_lines l ON l.product_id = p.product_id
      WHERE l.saga_id = :sagaId
      ORDER BY p.product_id
      FOR UPDATE OF p
      """;

  private final DatabaseClient database;
  private final OutboxStore outbox;

  public StockReservationService(DatabaseClient database, OutboxStore outbox) {
    this.database = database;
    this.outbox = outbox;
  }

  public Mono<Void> handle(MessageEnvelope envelope, SagaMessage message) {
    return switch (message) {
      case ReserveStock command -> reserve(envelope.sagaId(), command);
      case ReleaseStock command -> release(envelope.sagaId(), command);
      default -> Mono.error(new IllegalArgumentException("Unsupported stock command: " + envelope.type()));
    };
  }

  private Mono<Void> reserve(UUID sagaId, ReserveStock command) {
    return findStatus(sagaId).flatMap(existing -> existing
        .map(status -> status == ReservationStatus.RESERVED
            ? findLines(sagaId).flatMap(lines -> reply(sagaId, new StockReserved(command.orderId(), lines)))
            : reply(sagaId, new StockRejected(command.orderId(), "Reservation is already " + status)))
        .orElseGet(() -> reserveNew(sagaId, command)));
  }

  private Mono<Void> reserveNew(UUID sagaId, ReserveStock command) {
    Map<Integer, Integer> wanted = mergeLines(command.lines());
    return database.sql(LOCK_PRODUCTS)
        .bind("ids", wanted.keySet().toArray(Integer[]::new))
        .map(row -> Map.entry(row.get("product_id", Integer.class),
            new AvailableProduct(row.get("stock", Integer.class), row.get("price", BigDecimal.class))))
        .all()
        .collectMap(Map.Entry::getKey, Map.Entry::getValue)
        .flatMap(available -> findShortage(wanted, available)
            .map(reason -> insertReservation(sagaId, command.orderId(), ReservationStatus.REJECTED)
                .then(reply(sagaId, new StockRejected(command.orderId(), reason))))
            .orElseGet(() -> {
              List<PricedLine> lines = wanted.entrySet().stream()
                  .map(line -> new PricedLine(line.getKey(), line.getValue(), available.get(line.getKey()).price()))
                  .toList();
              return insertReservation(sagaId, command.orderId(), ReservationStatus.RESERVED)
                  .thenMany(Flux.fromIterable(lines))
                  .concatMap(line -> takeStock(sagaId, line))
                  .then(reply(sagaId, new StockReserved(command.orderId(), lines)));
            }));
  }

  private Mono<Void> release(UUID sagaId, ReleaseStock command) {
    return findStatus(sagaId).flatMap(existing -> {
      Mono<Void> change = existing
          .map(status -> status == ReservationStatus.RESERVED ? restore(sagaId) : Mono.<Void>empty())
          // Record the release so a late ReserveStock for this saga is rejected.
          .orElseGet(() -> insertReservation(sagaId, command.orderId(), ReservationStatus.RELEASED));
      return change.then(reply(sagaId, new StockReleased(command.orderId())));
    });
  }

  private Mono<Void> restore(UUID sagaId) {
    return database.sql(LOCK_RESERVED_PRODUCTS).bind("sagaId", sagaId).fetch().all().then(
        database.sql(RESTORE_LINES).bind("sagaId", sagaId).fetch().rowsUpdated()).then(
            database.sql(MARK_RELEASED).bind("sagaId", sagaId).fetch().rowsUpdated())
        .then();
  }

  private Mono<Void> takeStock(UUID sagaId, PricedLine line) {
    return database.sql(TAKE_STOCK)
        .bind("quantity", line.quantity())
        .bind("productId", line.productId())
        .fetch()
        .rowsUpdated()
        .flatMap(updated -> updated == 1
            ? database.sql(INSERT_LINE)
                .bind("sagaId", sagaId)
                .bind("productId", line.productId())
                .bind("quantity", line.quantity())
                .bind("unitPrice", line.unitPrice())
                .fetch()
                .rowsUpdated()
                .then()
            : Mono.error(new IllegalStateException("Locked product " + line.productId() + " lost stock")));
  }

  private Mono<List<PricedLine>> findLines(UUID sagaId) {
    return database.sql(FIND_LINES)
        .bind("sagaId", sagaId)
        .map(row -> new PricedLine(row.get("product_id", Integer.class), row.get("quantity", Integer.class),
            row.get("unit_price", BigDecimal.class)))
        .all()
        .collectList();
  }

  private Mono<Optional<ReservationStatus>> findStatus(UUID sagaId) {
    return database.sql(FIND_STATUS)
        .bind("sagaId", sagaId)
        .map(row -> Optional.of(ReservationStatus.valueOf(row.get("status", String.class))))
        .one()
        .defaultIfEmpty(Optional.empty());
  }

  private Mono<Void> insertReservation(UUID sagaId, UUID orderId, ReservationStatus status) {
    return database.sql(INSERT_RESERVATION)
        .bind("sagaId", sagaId)
        .bind("orderId", orderId)
        .bind("status", status.name())
        .fetch()
        .rowsUpdated()
        .then();
  }

  private Mono<Void> reply(UUID sagaId, SagaMessage message) {
    return outbox.append(SagaTopics.STOCK_REPLIES, sagaId, message).then();
  }

  /** Sorted by product id so concurrent reservations take locks in the same order. */
  private static Map<Integer, Integer> mergeLines(List<OrderLine> lines) {
    Map<Integer, Integer> merged = new TreeMap<>();
    lines.forEach(line -> merged.merge(line.productId(), line.quantity(), Math::addExact));
    return merged;
  }

  private static Optional<String> findShortage(Map<Integer, Integer> wanted, Map<Integer, AvailableProduct> available) {
    return wanted.entrySet().stream()
        .filter(line -> !available.containsKey(line.getKey())
            || available.get(line.getKey()).stock() < line.getValue())
        .findFirst()
        .map(line -> available.containsKey(line.getKey())
            ? "Insufficient stock for product " + line.getKey()
            : "Unknown product " + line.getKey());
  }

  private record AvailableProduct(int stock, BigDecimal price) {
  }
}
