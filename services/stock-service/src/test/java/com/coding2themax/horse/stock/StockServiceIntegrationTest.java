package com.coding2themax.horse.stock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.OrderLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReleaseStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReserveStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockRejected;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReleased;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReserved;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageCodec;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;
import com.coding2themax.horse.stock.application.StockReservationService;
import com.coding2themax.horse.stock.domain.Product;
import com.coding2themax.horse.stock.infrastructure.ProductRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "horse.saga.messaging.relay.poll-interval=50ms",
    "horse.saga.messaging.retry.attempts=1",
    "horse.saga.messaging.retry.backoff=10ms" })
@Testcontainers
class StockServiceIntegrationTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("DATABASE_HOST", () -> "r2dbc:postgresql://%s:%d/%s".formatted(POSTGRES.getHost(),
        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), POSTGRES.getDatabaseName()));
    registry.add("DATABASE_JDBC_URL", POSTGRES::getJdbcUrl);
    registry.add("DATABASE_USERNAME", POSTGRES::getUsername);
    registry.add("DATABASE_PASSWORD", POSTGRES::getPassword);
    registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
  }

  @Autowired
  private WebTestClient web;
  @Autowired
  private DatabaseClient database;
  @Autowired
  private ProductRepository products;
  @Autowired
  private StockReservationService reservations;
  @Autowired
  private SagaMessageHandler handler;
  @Autowired
  private SagaMessageCodec codec;
  @Autowired
  private KafkaTemplate<String, String> kafka;

  @BeforeEach
  void clean() {
    database.sql("TRUNCATE stock_reservation_lines, stock_reservations, products, outbox, inbox RESTART IDENTITY")
        .then().block(TIMEOUT);
  }

  @Test
  void reserveTakesStockAndReplies() {
    int product = product(5);
    UUID saga = UUID.randomUUID();

    process(saga, reserve(line(product, 2)));

    assertEquals(3, stock(product));
    assertInstanceOf(StockReserved.class, lastReply(saga));
  }

  @Test
  void shortageRejectsWholeOrderWithoutTakingStock() {
    int enough = product(5);
    int scarce = product(1);
    UUID saga = UUID.randomUUID();

    process(saga, reserve(line(enough, 2), line(scarce, 2)));

    assertEquals(5, stock(enough));
    assertEquals(1, stock(scarce));
    StockRejected reply = assertInstanceOf(StockRejected.class, lastReply(saga));
    assertTrue(reply.reason().contains("Insufficient stock"));
  }

  @Test
  void unknownProductIsRejected() {
    UUID saga = UUID.randomUUID();

    process(saga, reserve(line(999, 1)));

    StockRejected reply = assertInstanceOf(StockRejected.class, lastReply(saga));
    assertTrue(reply.reason().contains("Unknown product"));
  }

  @Test
  void repeatedProductLinesAreCheckedTogether() {
    int product = product(3);
    UUID saga = UUID.randomUUID();

    process(saga, reserve(line(product, 2), line(product, 2)));

    assertEquals(3, stock(product));
    assertInstanceOf(StockRejected.class, lastReply(saga));
  }

  @Test
  void duplicateDeliveryRunsOnce() {
    int product = product(5);
    UUID saga = UUID.randomUUID();
    String value = codec.toJson(codec.encode(saga, reserve(line(product, 2))));

    processValue(value);
    processValue(value);

    assertEquals(3, stock(product));
    assertEquals(1, replies(saga).size());
  }

  @Test
  void retriedCommandWithNewMessageIdDoesNotReserveTwice() {
    int product = product(5);
    UUID saga = UUID.randomUUID();
    ReserveStock command = reserve(line(product, 2));

    process(saga, command);
    process(saga, command);

    assertEquals(3, stock(product));
    assertEquals(2, replies(saga).size());
    assertInstanceOf(StockReserved.class, lastReply(saga));
  }

  @Test
  void releaseRestoresStockOnce() {
    int first = product(5);
    int second = product(4);
    UUID saga = UUID.randomUUID();
    ReserveStock command = reserve(line(first, 2), line(second, 1));
    process(saga, command);

    process(saga, new ReleaseStock(command.orderId()));
    process(saga, new ReleaseStock(command.orderId()));

    assertEquals(5, stock(first));
    assertEquals(4, stock(second));
    assertInstanceOf(StockReleased.class, lastReply(saga));
  }

  @Test
  void releaseOfRejectedReservationDoesNotAddStock() {
    int product = product(1);
    UUID saga = UUID.randomUUID();
    ReserveStock command = reserve(line(product, 2));
    process(saga, command);

    process(saga, new ReleaseStock(command.orderId()));

    assertEquals(1, stock(product));
    assertInstanceOf(StockReleased.class, lastReply(saga));
  }

  @Test
  void lateReserveAfterReleaseIsRejected() {
    int product = product(5);
    UUID saga = UUID.randomUUID();
    ReserveStock command = reserve(line(product, 2));

    process(saga, new ReleaseStock(command.orderId()));
    process(saga, command);

    assertEquals(5, stock(product));
    assertInstanceOf(StockRejected.class, lastReply(saga));
  }

  @Test
  void concurrentOrdersCannotOversellLastUnit() {
    int product = product(1);
    List<UUID> sagas = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      sagas.add(UUID.randomUUID());
    }

    Flux.fromIterable(sagas)
        .flatMap(saga -> processAsync(saga, reserve(line(product, 1))).subscribeOn(Schedulers.parallel()), 8)
        .then().block(TIMEOUT);

    assertEquals(0, stock(product));
    assertEquals(1, sagas.stream().filter(saga -> lastReply(saga) instanceof StockReserved).count());
    assertEquals(7, sagas.stream().filter(saga -> lastReply(saga) instanceof StockRejected).count());
  }

  @Test
  void oppositeLineOrderDoesNotDeadlock() {
    int first = product(50);
    int second = product(50);

    Flux.range(0, 20)
        .flatMap(i -> processAsync(UUID.randomUUID(), i % 2 == 0
            ? reserve(line(first, 1), line(second, 1))
            : reserve(line(second, 1), line(first, 1))).subscribeOn(Schedulers.parallel()), 8)
        .then().block(TIMEOUT);

    assertEquals(30, stock(first));
    assertEquals(30, stock(second));
    assertEquals(0L, database.sql("SELECT count(*) FROM stock_reservations WHERE status <> 'RESERVED'")
        .map(row -> row.get(0, Long.class)).one().block(TIMEOUT));
  }

  @Test
  void unsupportedCommandFails() {
    SagaMessage message = new StockReserved(UUID.randomUUID());
    MessageEnvelope envelope = codec.encode(UUID.randomUUID(), message);

    assertThrows(IllegalArgumentException.class, () -> reservations.handle(envelope, message).block(TIMEOUT));
  }

  @Test
  void commandTravelsThroughKafkaAndReplyIsPublished() {
    int product = product(5);
    UUID saga = UUID.randomUUID();
    ReserveStock command = reserve(line(product, 2));

    kafka.send(SagaTopics.STOCK_COMMANDS, saga.toString(), codec.toJson(codec.encode(saga, command))).join();

    ConsumerRecord<String, String> reply = readReply(saga);
    assertInstanceOf(StockReserved.class, codec.decode(codec.fromJson(reply.value())));
    assertEquals(3, stock(product));
  }

  @Test
  void productApiCreatesReadsAndUpdates() {
    Integer id = web.post().uri("/products").contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("name", "Saddle", "description", "Leather", "price", 99.50, "stock", 4))
        .exchange()
        .expectStatus().isCreated()
        .expectHeader().exists("Location")
        .expectBody(Map.class).returnResult().getResponseBody().get("id") instanceof Integer value ? value : null;

    web.get().uri("/products").exchange().expectStatus().isOk().expectBody().jsonPath("$.length()").isEqualTo(1);
    web.get().uri("/products/{id}", id).exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.name").isEqualTo("Saddle").jsonPath("$.price").isEqualTo(99.50);

    web.put().uri("/products/{id}", id).contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("name", "Western Saddle", "price", 120))
        .exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.name").isEqualTo("Western Saddle").jsonPath("$.stock").isEqualTo(4);

    web.post().uri("/products/{id}/stock-adjustments", id).contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("delta", 6))
        .exchange().expectStatus().isOk().expectBody().jsonPath("$.stock").isEqualTo(10);
  }

  @Test
  void productApiRejectsInvalidRequests() {
    int product = product(2);

    web.get().uri("/products/abc").exchange().expectStatus().isBadRequest();
    web.get().uri("/products/999").exchange().expectStatus().isNotFound();
    web.post().uri("/products").contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("name", " ", "price", 1, "stock", 1)).exchange().expectStatus().isBadRequest();
    web.post().uri("/products").contentType(MediaType.APPLICATION_JSON).exchange().expectStatus().isBadRequest();
    web.put().uri("/products/999").contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("name", "Missing", "price", 1)).exchange().expectStatus().isNotFound();
    web.post().uri("/products/{id}/stock-adjustments", product).contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("delta", -3)).exchange().expectStatus().isEqualTo(409);
    web.post().uri("/products/{id}/stock-adjustments", product).contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("delta", Integer.MAX_VALUE)).exchange().expectStatus().isEqualTo(409);
    web.post().uri("/products/999/stock-adjustments").contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("delta", 1)).exchange().expectStatus().isNotFound();
    assertEquals(2, stock(product));
  }

  private int product(int stock) {
    Product product = new Product();
    product.setName("Product");
    product.setPrice(BigDecimal.TEN);
    product.setStock(stock);
    return products.save(product).block(TIMEOUT).getId();
  }

  private int stock(int product) {
    return products.findById(product).block(TIMEOUT).getStock();
  }

  private static OrderLine line(int product, int quantity) {
    return new OrderLine(product, quantity);
  }

  private static ReserveStock reserve(OrderLine... lines) {
    return new ReserveStock(UUID.randomUUID(), List.of(lines));
  }

  private void process(UUID saga, SagaMessage message) {
    processAsync(saga, message).block(TIMEOUT);
  }

  private Mono<Void> processAsync(UUID saga, SagaMessage message) {
    return handleValue(codec.toJson(codec.encode(saga, message)));
  }

  private void processValue(String value) {
    handleValue(value).block(TIMEOUT);
  }

  private Mono<Void> handleValue(String value) {
    return handler.handle(new ConsumerRecord<>("test.commands", 0, 0L, "key", value), reservations::handle);
  }

  private List<SagaMessage> replies(UUID saga) {
    return database.sql("SELECT envelope FROM outbox WHERE saga_id = :saga AND topic = :topic ORDER BY id")
        .bind("saga", saga)
        .bind("topic", SagaTopics.STOCK_REPLIES)
        .map(row -> codec.decode(codec.fromJson(row.get("envelope", String.class))))
        .all().collectList().block(TIMEOUT);
  }

  private SagaMessage lastReply(UUID saga) {
    List<SagaMessage> replies = replies(saga);
    return replies.get(replies.size() - 1);
  }

  private static ConsumerRecord<String, String> readReply(UUID saga) {
    Map<String, Object> config = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
        new StringDeserializer())) {
      consumer.subscribe(List.of(SagaTopics.STOCK_REPLIES));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
          if (saga.toString().equals(record.key())) {
            return record;
          }
        }
      }
    }
    throw new AssertionError("No reply for saga " + saga);
  }
}
