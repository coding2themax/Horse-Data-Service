package com.coding2themax.horse.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import com.coding2themax.horse.order.api.dto.OrderResponse;
import com.coding2themax.horse.order.application.OrderCommandService;
import com.coding2themax.horse.order.application.OrderService;
import com.coding2themax.horse.order.domain.OrderStatus;
import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.PricedLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ApproveOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.CancelOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApprovalFailed;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApproved;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderCancelled;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderPlaced;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageCodec;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "horse.saga.messaging.relay.poll-interval=50ms",
    "horse.saga.messaging.retry.attempts=1",
    "horse.saga.messaging.retry.backoff=10ms" })
@Testcontainers
class OrderServiceIntegrationTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final String IDEMPOTENCY_KEY = "X-Idempotency-Key";
  private static final String IDEMPOTENCY_REPLAYED = "X-Idempotency-Replayed";

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
  private OrderService orders;
  @Autowired
  private OrderCommandService commands;
  @Autowired
  private SagaMessageHandler handler;
  @Autowired
  private SagaMessageCodec codec;
  @Autowired
  private KafkaTemplate<String, String> kafka;
  @Autowired
  private ReactiveTransactionManager transactionManager;

  @BeforeEach
  void clean() {
    database.sql("TRUNCATE order_items, orders, customers, outbox, inbox RESTART IDENTITY").then().block(TIMEOUT);
  }

  @Test
  void customerApiCreatesAndReads() {
    int id = customer("rider@example.com");

    web.get().uri("/customers/{id}", id).exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.firstName").isEqualTo("Sam").jsonPath("$.lastName").isEqualTo("Rider")
        .jsonPath("$.email").isEqualTo("rider@example.com").jsonPath("$.phone").isEqualTo("5551234");
  }

  @Test
  void customerApiRejectsInvalidRequests() {
    customer("taken@example.com");

    postCustomer("taken@example.com").expectStatus().isEqualTo(409);
    postCustomer("not-an-email").expectStatus().isBadRequest();
    web.post().uri("/customers").contentType(MediaType.APPLICATION_JSON).exchange().expectStatus().isBadRequest();
    web.get().uri("/customers/abc").exchange().expectStatus().isBadRequest();
    web.get().uri("/customers/999").exchange().expectStatus().isNotFound();
  }

  @Test
  void placingOrderStoresItAndStartsSaga() {
    int customer = customer("buyer@example.com");

    OrderResponse order = web.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
        .header(IDEMPOTENCY_KEY, UUID.randomUUID().toString())
        .bodyValue(Map.of("customerId", customer, "lines", List.of(
            Map.of("productId", 7, "quantity", 1), Map.of("productId", 3, "quantity", 2),
            Map.of("productId", 7, "quantity", 4))))
        .exchange()
        .expectStatus().isAccepted()
        .expectHeader().exists("Location")
        .expectBody(OrderResponse.class).returnResult().getResponseBody();

    assertEquals(OrderStatus.PLACED, order.status());
    assertNull(order.totalPrice());
    assertEquals(List.of(3, 7), order.items().stream().map(item -> item.productId()).toList());
    assertEquals(5, order.items().get(1).quantity());
    OrderPlaced event = assertInstanceOf(OrderPlaced.class, outboxMessages(order.sagaId()).get(0));
    assertEquals(order.id(), event.orderId());
    assertEquals(2, event.lines().size());
    web.get().uri("/orders/{id}", order.id()).exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.status").isEqualTo("PLACED");
  }

  @Test
  void orderRequiresValidIdempotencyKey() {
    int customer = customer("key@example.com");
    Map<String, Object> body = orderBody(customer, 1, 1);

    web.post().uri("/orders").contentType(MediaType.APPLICATION_JSON).bodyValue(body).exchange()
        .expectStatus().isBadRequest();
    postOrder("not-a-uuid", body).expectStatus().isBadRequest();
    postOrder("1-1-1-1-1", body).expectStatus().isBadRequest();

    assertEquals(0, rows("orders"));
    assertEquals(0, rows("outbox"));
  }

  @Test
  void retryReturnsOriginalOrderWithoutStartingAnotherSaga() {
    int customer = customer("retry@example.com");
    String key = UUID.randomUUID().toString();
    Map<String, Object> body = orderBody(customer, 1, 2);

    OrderResponse first = accepted(postOrder(key, body), "false");
    OrderResponse retry = accepted(postOrder(key, body), "true");

    assertEquals(first.id(), retry.id());
    assertEquals(first.sagaId(), retry.sagaId());
    assertEquals(1, rows("orders"));
    assertEquals(1, rows("order_items"));
    assertEquals(1, rows("outbox"));
  }

  @Test
  void retryWithEquivalentLinesInAnotherOrderIsReplay() {
    int customer = customer("equivalent@example.com");
    String key = UUID.randomUUID().toString();

    OrderResponse first = accepted(postOrder(key, Map.of("customerId", customer, "lines", List.of(
        Map.of("productId", 1, "quantity", 1), Map.of("productId", 2, "quantity", 3)))), "false");
    OrderResponse retry = accepted(postOrder(key, Map.of("customerId", customer, "lines", List.of(
        Map.of("productId", 2, "quantity", 3), Map.of("productId", 1, "quantity", 1)))), "true");

    assertEquals(first.id(), retry.id());
  }

  @Test
  void reusedKeyWithDifferentRequestIsRejected() {
    int customer = customer("reuse@example.com");
    int other = customer("other@example.com");
    String key = UUID.randomUUID().toString();
    accepted(postOrder(key, orderBody(customer, 1, 1)), "false");

    postOrder(key, orderBody(customer, 1, 2)).expectStatus().isEqualTo(422);
    postOrder(key, orderBody(customer, 2, 1)).expectStatus().isEqualTo(422);
    postOrder(key, orderBody(other, 1, 1)).expectStatus().isEqualTo(422);

    assertEquals(1, rows("orders"));
    assertEquals(1, rows("outbox"));
  }

  @Test
  void failedRequestDoesNotConsumeKey() {
    String key = UUID.randomUUID().toString();
    postOrder(key, orderBody(999, 1, 1)).expectStatus().isEqualTo(422);
    int customer = customer("after-failure@example.com");

    accepted(postOrder(key, orderBody(customer, 1, 1)), "false");

    assertEquals(1, rows("orders"));
  }

  @Test
  void concurrentRetriesCreateOneOrder() throws Exception {
    int customer = customer("concurrent@example.com");
    String key = UUID.randomUUID().toString();
    Map<String, Object> body = orderBody(customer, 1, 1);
    List<Callable<OrderResponse>> requests = Collections.nCopies(12, () -> postOrder(key, body)
        .expectStatus().isAccepted()
        .expectBody(OrderResponse.class).returnResult().getResponseBody());

    Set<UUID> ids = new HashSet<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(12)) {
      for (Future<OrderResponse> response : pool.invokeAll(requests)) {
        ids.add(response.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).id());
      }
    }

    assertEquals(1, ids.size());
    assertEquals(1, rows("orders"));
    assertEquals(1, rows("order_items"));
    assertEquals(1, rows("outbox"));
  }

  @Test
  void invalidOrdersCreateNothing() {
    int customer = customer("invalid@example.com");

    postOrder(Map.of("customerId", 999, "lines", List.of(Map.of("productId", 1, "quantity", 1))))
        .expectStatus().isEqualTo(422);
    postOrder(Map.of("customerId", customer, "lines", List.of())).expectStatus().isBadRequest();
    postOrder(Map.of("customerId", customer, "lines", List.of(Map.of("productId", 1, "quantity", 0))))
        .expectStatus().isBadRequest();
    postOrder(Map.of("customerId", customer, "lines", List.of(
        Map.of("productId", 1, "quantity", Integer.MAX_VALUE), Map.of("productId", 1, "quantity", 1))))
        .expectStatus().isBadRequest();
    web.get().uri("/orders/not-a-uuid").exchange().expectStatus().isBadRequest();
    web.get().uri("/orders/{id}", UUID.randomUUID()).exchange().expectStatus().isNotFound();

    assertEquals(0, count("SELECT count(*) FROM orders"));
    assertEquals(0, count("SELECT count(*) FROM outbox"));
  }

  @Test
  void approvalSnapshotsPricesAndTotal() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2), Map.of("productId", 7, "quantity", 1));

    process(order.sagaId(), approve(order, priced(3, 2, "10.25"), priced(7, 1, "5.00")));

    OrderResponse approved = orders.findById(order.id()).block(TIMEOUT);
    assertEquals(OrderStatus.APPROVED, approved.status());
    assertEquals(0, new BigDecimal("25.50").compareTo(approved.totalPrice()));
    assertEquals(0, new BigDecimal("10.25").compareTo(approved.items().get(0).unitPrice()));
    assertInstanceOf(OrderApproved.class, lastReply(order.sagaId()));
  }

  @Test
  void retriedApprovalKeepsOriginalPrices() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));
    process(order.sagaId(), approve(order, priced(3, 2, "10.00")));

    process(order.sagaId(), approve(order, priced(3, 2, "99.00")));

    assertEquals(0, new BigDecimal("20.00").compareTo(orders.findById(order.id()).block(TIMEOUT).totalPrice()));
    assertInstanceOf(OrderApproved.class, lastReply(order.sagaId()));
  }

  @Test
  void duplicateDeliveryRunsOnce() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));
    String value = codec.toJson(codec.encode(order.sagaId(), approve(order, priced(3, 2, "10.00"))));

    processValue(value);
    processValue(value);

    assertEquals(1, replies(order.sagaId()).size());
  }

  @Test
  void mismatchedLinesFailApproval() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));

    process(order.sagaId(), approve(order, priced(3, 1, "10.00")));

    assertEquals(OrderStatus.PLACED, orders.findById(order.id()).block(TIMEOUT).status());
    assertInstanceOf(OrderApprovalFailed.class, lastReply(order.sagaId()));
  }

  @Test
  void duplicateProductLinesFailApproval() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));

    process(order.sagaId(), approve(order, priced(3, 1, "10.00"), priced(3, 1, "10.00")));

    assertEquals(OrderStatus.PLACED, orders.findById(order.id()).block(TIMEOUT).status());
    assertInstanceOf(OrderApprovalFailed.class, lastReply(order.sagaId()));
  }

  @Test
  void cancelledOrUnknownOrderCannotBeApproved() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));
    process(order.sagaId(), new CancelOrder(order.id(), "out of stock"));

    process(order.sagaId(), approve(order, priced(3, 2, "10.00")));

    assertEquals(OrderStatus.CANCELLED, orders.findById(order.id()).block(TIMEOUT).status());
    assertInstanceOf(OrderApprovalFailed.class, lastReply(order.sagaId()));

    UUID saga = UUID.randomUUID();
    process(saga, new ApproveOrder(UUID.randomUUID(), List.of(priced(3, 2, "10.00"))));
    OrderApprovalFailed failed = assertInstanceOf(OrderApprovalFailed.class, lastReply(saga));
    assertTrue(failed.reason().contains("not found"));
  }

  @Test
  void cancelIsIdempotentAndKeepsReason() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));

    process(order.sagaId(), new CancelOrder(order.id(), "out of stock"));
    process(order.sagaId(), new CancelOrder(order.id(), "another reason"));

    OrderResponse cancelled = orders.findById(order.id()).block(TIMEOUT);
    assertEquals(OrderStatus.CANCELLED, cancelled.status());
    assertEquals("out of stock", cancelled.statusReason());
    assertInstanceOf(OrderCancelled.class, lastReply(order.sagaId()));
  }

  @Test
  void approvedOrderCanBeCompensated() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));
    process(order.sagaId(), approve(order, priced(3, 2, "10.00")));

    process(order.sagaId(), new CancelOrder(order.id(), null));

    OrderResponse cancelled = orders.findById(order.id()).block(TIMEOUT);
    assertEquals(OrderStatus.CANCELLED, cancelled.status());
    assertEquals("Order saga cancelled", cancelled.statusReason());
  }

  @Test
  void invalidCommandsFail() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));
    database.sql("UPDATE orders SET status = 'DELIVERED' WHERE order_id = :id").bind("id", order.id())
        .then().block(TIMEOUT);
    SagaMessage delivered = new CancelOrder(order.id(), "late");
    SagaMessage unknown = new CancelOrder(UUID.randomUUID(), "missing");
    SagaMessage unsupported = new OrderApproved(order.id());

    assertThrows(IllegalStateException.class, () -> direct(order.sagaId(), delivered));
    assertThrows(IllegalStateException.class, () -> direct(order.sagaId(), unknown));
    assertThrows(IllegalArgumentException.class, () -> direct(order.sagaId(), unsupported));
    process(order.sagaId(), approve(order, priced(3, 2, "10.00")));
    assertInstanceOf(OrderApprovalFailed.class, lastReply(order.sagaId()));
  }

  @Test
  void orderEventAndCommandReplyTravelThroughKafka() {
    OrderResponse order = order(Map.of("productId", 3, "quantity", 2));

    assertInstanceOf(OrderPlaced.class, read(SagaTopics.ORDER_EVENTS, order.sagaId()));

    kafka.send(SagaTopics.ORDER_COMMANDS, order.sagaId().toString(),
        codec.toJson(codec.encode(order.sagaId(), approve(order, priced(3, 2, "10.00"))))).join();

    assertInstanceOf(OrderApproved.class, read(SagaTopics.ORDER_REPLIES, order.sagaId()));
    assertEquals(OrderStatus.APPROVED, orders.findById(order.id()).block(TIMEOUT).status());
  }

  private int customer(String email) {
    return (Integer) postCustomer(email).expectStatus().isCreated()
        .expectBody(Map.class).returnResult().getResponseBody().get("id");
  }

  private WebTestClient.ResponseSpec postCustomer(String email) {
    return web.post().uri("/customers").contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("firstName", "Sam", "lastName", "Rider", "email", email, "phone", "5551234"))
        .exchange();
  }

  private WebTestClient.ResponseSpec postOrder(Map<String, Object> body) {
    return postOrder(UUID.randomUUID().toString(), body);
  }

  private WebTestClient.ResponseSpec postOrder(String key, Object body) {
    return web.post().uri("/orders").contentType(MediaType.APPLICATION_JSON)
        .header(IDEMPOTENCY_KEY, key).bodyValue(body).exchange();
  }

  private static Map<String, Object> orderBody(int customer, int product, int quantity) {
    return Map.of("customerId", customer, "lines", List.of(Map.of("productId", product, "quantity", quantity)));
  }

  private OrderResponse accepted(WebTestClient.ResponseSpec response, String replayed) {
    return response.expectStatus().isAccepted()
        .expectHeader().valueEquals(IDEMPOTENCY_REPLAYED, replayed)
        .expectBody(OrderResponse.class).returnResult().getResponseBody();
  }

  private long rows(String table) {
    return database.sql("SELECT count(*) AS total FROM " + table)
        .map(row -> row.get("total", Long.class)).one().block(TIMEOUT);
  }

  @SafeVarargs
  private OrderResponse order(Map<String, Integer>... lines) {
    int customer = customer(UUID.randomUUID() + "@example.com");
    return postOrder(Map.of("customerId", customer, "lines", List.of(lines)))
        .expectStatus().isAccepted()
        .expectBody(OrderResponse.class).returnResult().getResponseBody();
  }

  private static PricedLine priced(int product, int quantity, String price) {
    return new PricedLine(product, quantity, new BigDecimal(price));
  }

  private static ApproveOrder approve(OrderResponse order, PricedLine... lines) {
    return new ApproveOrder(order.id(), List.of(lines));
  }

  private void process(UUID saga, SagaMessage message) {
    processValue(codec.toJson(codec.encode(saga, message)));
  }

  private void processValue(String value) {
    handler.handle(new ConsumerRecord<>("test.commands", 0, 0L, "key", value), commands::handle).block(TIMEOUT);
  }

  private void direct(UUID saga, SagaMessage message) {
    MessageEnvelope envelope = codec.encode(saga, message);
    TransactionalOperator.create(transactionManager).transactional(commands.handle(envelope, message)).block(TIMEOUT);
  }

  private List<SagaMessage> outboxMessages(UUID saga) {
    return database.sql("SELECT envelope FROM outbox WHERE saga_id = :saga ORDER BY id")
        .bind("saga", saga)
        .map(row -> codec.decode(codec.fromJson(row.get("envelope", String.class))))
        .all().collectList().block(TIMEOUT);
  }

  private List<SagaMessage> replies(UUID saga) {
    return database.sql("SELECT envelope FROM outbox WHERE saga_id = :saga AND topic = :topic ORDER BY id")
        .bind("saga", saga)
        .bind("topic", SagaTopics.ORDER_REPLIES)
        .map(row -> codec.decode(codec.fromJson(row.get("envelope", String.class))))
        .all().collectList().block(TIMEOUT);
  }

  private SagaMessage lastReply(UUID saga) {
    List<SagaMessage> replies = replies(saga);
    return replies.get(replies.size() - 1);
  }

  private long count(String sql) {
    return database.sql(sql).map(row -> row.get(0, Long.class)).one().block(TIMEOUT);
  }

  private SagaMessage read(String topic, UUID saga) {
    Map<String, Object> config = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
        new StringDeserializer())) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
          if (saga.toString().equals(record.key())) {
            return codec.decode(codec.fromJson(record.value()));
          }
        }
      }
    }
    throw new AssertionError("No message on " + topic + " for saga " + saga);
  }
}
