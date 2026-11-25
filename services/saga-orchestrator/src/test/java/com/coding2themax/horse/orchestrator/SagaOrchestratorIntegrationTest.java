package com.coding2themax.horse.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
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

import com.coding2themax.horse.orchestrator.api.dto.SagaResponse;
import com.coding2themax.horse.orchestrator.application.OrchestratorProperties;
import com.coding2themax.horse.orchestrator.application.SagaAttentionMonitor;
import com.coding2themax.horse.orchestrator.application.SagaOrchestrator;
import com.coding2themax.horse.orchestrator.application.SagaTimeoutSweeper;
import com.coding2themax.horse.orchestrator.domain.SagaDefinition;
import com.coding2themax.horse.orchestrator.domain.SagaInstance;
import com.coding2themax.horse.orchestrator.domain.SagaState;
import com.coding2themax.horse.orchestrator.domain.StuckStep;
import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;
import com.coding2themax.horse.saga.contracts.OrderLine;
import com.coding2themax.horse.saga.contracts.PricedLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ApproveOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.CancelOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApprovalFailed;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApproved;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderCancelled;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderPlaced;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReleaseStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReserveStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockRejected;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReleased;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReserved;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageCodec;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "horse.saga.messaging.relay.poll-interval=50ms",
    "horse.saga.messaging.retry.attempts=0",
    "horse.saga.messaging.retry.backoff=10ms",
    "horse.saga.orchestrator.sweep-enabled=false",
    "horse.saga.orchestrator.max-retries=2" })
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class SagaOrchestratorIntegrationTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final List<OrderLine> LINES = List.of(new OrderLine(7, 2));
  private static final List<PricedLine> PRICED = List.of(new PricedLine(7, 2, new BigDecimal("10.00")));

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
  private SagaOrchestrator orchestrator;
  @Autowired
  private SagaStore store;
  @Autowired
  private SagaTimeoutSweeper sweeper;
  @Autowired
  private SagaAttentionMonitor attentionMonitor;
  @Autowired
  private SagaMessageHandler handler;
  @Autowired
  private SagaMessageCodec codec;
  @Autowired
  private KafkaTemplate<String, String> kafka;
  @Autowired
  private ReactiveTransactionManager transactionManager;

  private UUID saga;
  private UUID order;

  @BeforeEach
  void setUp() {
    database.sql("TRUNCATE saga_step_log, saga_instance, outbox, inbox").then().block(TIMEOUT);
    saga = UUID.randomUUID();
    order = UUID.randomUUID();
  }

  @Test
  void happyPathCompletes() {
    receive(new OrderPlaced(order, LINES));
    assertEquals(SagaState.RESERVING_STOCK, state());
    ReserveStock reserve = assertInstanceOf(ReserveStock.class, lastCommand());
    assertEquals(LINES, reserve.lines());

    receive(new StockReserved(order, PRICED));
    assertEquals(SagaState.APPROVING_ORDER, state());
    ApproveOrder approve = assertInstanceOf(ApproveOrder.class, lastCommand());
    assertEquals(PRICED.size(), approve.lines().size());

    receive(new OrderApproved(order));
    assertEquals(SagaState.COMPLETED, state());
    assertNull(view().failureReason());
    assertEquals(3, view().steps().size());
  }

  @Test
  void rejectedStockCancelsOrderWithoutRelease() {
    receive(new OrderPlaced(order, LINES));

    receive(new StockRejected(order, "Insufficient stock"));
    CancelOrder cancel = assertInstanceOf(CancelOrder.class, lastCommand());
    assertEquals("Insufficient stock", cancel.reason());

    receive(new OrderCancelled(order));
    assertEquals(SagaState.CANCELLED, state());
    assertEquals("Insufficient stock", view().failureReason());
    assertEquals(0, commandCount(ReleaseStock.class));
  }

  @Test
  void failedApprovalReleasesStockThenCancels() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockReserved(order, PRICED));

    receive(new OrderApprovalFailed(order, "Order is CANCELLED"));
    assertEquals(SagaState.RELEASING_STOCK, state());
    assertInstanceOf(ReleaseStock.class, lastCommand());

    receive(new StockReleased(order));
    assertEquals(SagaState.CANCELLING_ORDER, state());
    CancelOrder cancel = assertInstanceOf(CancelOrder.class, lastCommand());
    assertEquals("Order is CANCELLED", cancel.reason());

    receive(new OrderCancelled(order));
    assertEquals(SagaState.CANCELLED, state());
  }

  @Test
  void duplicateOrderPlacedDoesNotRestartSaga() {
    receive(new OrderPlaced(order, LINES));
    receive(new OrderPlaced(order, LINES));

    assertEquals(1, commandCount(ReserveStock.class));
    assertEquals(1, view().steps().size());
  }

  @Test
  void repeatedMessageIdIsProcessedOnce() {
    receive(new OrderPlaced(order, LINES));
    String value = codec.toJson(codec.encode(saga, new StockReserved(order, PRICED)));

    handle(value);
    handle(value);

    assertEquals(1, commandCount(ApproveOrder.class));
    assertEquals(2, view().steps().size());
  }

  @Test
  void lateRepliesAreLoggedAndIgnored() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockReserved(order, PRICED));
    receive(new OrderApproved(order));

    receive(new StockReserved(order, PRICED));
    receive(new OrderApprovalFailed(order, "late"));

    assertEquals(SagaState.COMPLETED, state());
    assertEquals(1, commandCount(ApproveOrder.class));
    assertEquals(0, commandCount(ReleaseStock.class));
    assertTrue(view().steps().get(4).detail().contains("Ignored"));
  }

  @Test
  void invalidRepliesFail() {
    receive(new OrderPlaced(order, LINES));
    SagaMessage otherOrder = new StockReleased(UUID.randomUUID());
    SagaMessage unknownSaga = new StockReleased(order);

    assertThrows(IllegalStateException.class, () -> inTransaction(saga, otherOrder));
    assertThrows(IllegalStateException.class, () -> inTransaction(UUID.randomUUID(), unknownSaga));
    assertEquals(SagaState.RESERVING_STOCK, state());
  }

  @Test
  void timeoutRetriesWithNewMessageIdThenCompensates() {
    receive(new OrderPlaced(order, LINES));

    expireAndSweep();
    expireAndSweep();

    assertEquals(SagaState.RESERVING_STOCK, state());
    assertEquals(2, view().attempts());
    assertEquals(3, commandCount(ReserveStock.class));
    List<String> ids = database.sql("SELECT message_id::text FROM outbox")
        .map(row -> row.get(0, String.class)).all().collectList().block(TIMEOUT);
    assertEquals(3, Set.copyOf(ids).size());

    expireAndSweep();

    assertEquals(SagaState.RELEASING_STOCK, state());
    assertEquals(0, view().attempts());
    assertInstanceOf(ReleaseStock.class, lastCommand());
    assertEquals("Timed out in RESERVING_STOCK", view().failureReason());
  }

  @Test
  void approvalTimeoutReleasesStockAndLateApprovalIsIgnored() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockReserved(order, PRICED));
    expireAndSweep();
    ApproveOrder retried = assertInstanceOf(ApproveOrder.class, lastCommand());
    assertEquals(0, new BigDecimal("10.00").compareTo(retried.lines().get(0).unitPrice()));
    expireAndSweep();
    expireAndSweep();
    assertEquals(SagaState.RELEASING_STOCK, state());

    receive(new OrderApproved(order));

    assertEquals(SagaState.RELEASING_STOCK, state());
  }

  @Test
  void stuckCompensationNeedsAttention() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockRejected(order, "Insufficient stock"));

    expireAndSweep();
    expireAndSweep();
    expireAndSweep();

    assertEquals(SagaState.FAILED_NEEDS_ATTENTION, state());
    assertTrue(view().failureReason().contains("Compensation timed out in CANCELLING_ORDER"));
    assertEquals(0L, sweeper.sweepOnce().block(TIMEOUT));
  }

  @Test
  void retryResumesStuckCancelWithOriginalReason() {
    stuckInCancelling();

    web.post().uri("/sagas/{id}/retry", saga).exchange().expectStatus().isAccepted()
        .expectBody().jsonPath("$.state").isEqualTo("CANCELLING_ORDER")
        .jsonPath("$.failureReason").isEqualTo("Insufficient stock");

    CancelOrder cancel = assertInstanceOf(CancelOrder.class, lastCommand());
    assertEquals("Insufficient stock", cancel.reason());
    receive(new OrderCancelled(order));
    assertEquals(SagaState.CANCELLED, state());
    assertEquals("Insufficient stock", view().failureReason());
  }

  @Test
  void retryResumesStuckReleaseThenCancels() {
    stuckInReleasing();

    retry().expectStatus().isAccepted();

    assertInstanceOf(ReleaseStock.class, lastCommand());
    assertEquals(SagaState.RELEASING_STOCK, state());
    receive(new StockReleased(order));
    CancelOrder cancel = assertInstanceOf(CancelOrder.class, lastCommand());
    assertEquals("Price changed", cancel.reason());
    receive(new OrderCancelled(order));
    assertEquals(SagaState.CANCELLED, state());
  }

  @Test
  void repeatedStallKeepsOriginalReasonAndCanRetryAgain() {
    stuckInCancelling();
    retry().expectStatus().isAccepted();
    expireAndSweep();
    expireAndSweep();
    expireAndSweep();
    assertEquals(SagaState.FAILED_NEEDS_ATTENTION, state());

    retry().expectStatus().isAccepted();

    assertEquals("Insufficient stock", view().failureReason());
    assertEquals("Insufficient stock", assertInstanceOf(CancelOrder.class, lastCommand()).reason());
  }

  @Test
  void lateReplyWhileStuckDoesNotChangeRetriedStep() {
    stuckInReleasing();
    receive(new StockReleased(order));
    assertEquals(SagaState.FAILED_NEEDS_ATTENTION, state());

    retry().expectStatus().isAccepted();

    assertEquals(SagaState.RELEASING_STOCK, state());
    assertInstanceOf(ReleaseStock.class, lastCommand());
  }

  @Test
  void retryRejectsSagasThatAreNotStuck() {
    receive(new OrderPlaced(order, LINES));
    retry().expectStatus().isEqualTo(409);
    assertEquals(1, commands().size());

    web.post().uri("/sagas/not-a-uuid/retry").exchange().expectStatus().isBadRequest();
    web.post().uri("/sagas/{id}/retry", UUID.randomUUID()).exchange().expectStatus().isNotFound();
  }

  @Test
  void repeatedRetryRequestSendsOneCommand() {
    stuckInCancelling();
    long before = commandCount(CancelOrder.class);

    retry().expectStatus().isAccepted();
    retry().expectStatus().isEqualTo(409);

    assertEquals(before + 1, commandCount(CancelOrder.class));
  }

  @Test
  void stuckSagaAlertsOnceAndAgainAfterNewStall(CapturedOutput output) {
    stuckInCancelling();
    String alert = "Saga needs attention: sagaId=" + saga;

    assertEquals(1L, attentionMonitor.checkOnce().block(TIMEOUT));
    assertEquals(1L, attentionMonitor.checkOnce().block(TIMEOUT));
    assertEquals(1, occurrences(output.getOut(), alert));

    retry().expectStatus().isAccepted();
    assertEquals(0L, attentionMonitor.checkOnce().block(TIMEOUT));
    expireAndSweep();
    expireAndSweep();
    expireAndSweep();
    assertEquals(1L, attentionMonitor.checkOnce().block(TIMEOUT));
    assertEquals(2, occurrences(output.getOut(), alert));
  }

  @Test
  void attentionApiAndMetricReportStuckSagas() {
    web.get().uri("/sagas/attention").exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.total").isEqualTo(0).jsonPath("$.sagas.length()").isEqualTo(0);
    stuckInCancelling();
    attentionMonitor.checkOnce().block(TIMEOUT);

    web.get().uri("/sagas/attention").exchange().expectStatus().isOk()
        .expectBody()
        .jsonPath("$.total").isEqualTo(1)
        .jsonPath("$.sagas[0].sagaId").isEqualTo(saga.toString())
        .jsonPath("$.sagas[0].orderId").isEqualTo(order.toString());
    web.get().uri("/actuator/metrics/saga.needs.attention").exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.measurements[0].value").isEqualTo(1.0);

    retry().expectStatus().isAccepted();
    attentionMonitor.checkOnce().block(TIMEOUT);
    web.get().uri("/actuator/metrics/saga.needs.attention").exchange().expectStatus().isOk()
        .expectBody().jsonPath("$.measurements[0].value").isEqualTo(0.0);
  }

  @Test
  void definitionRejectsManualRetryOfForwardStep() {
    SagaInstance stuck = new SagaInstance(saga, order, SagaState.FAILED_NEEDS_ATTENTION, null, "failed", 0);

    assertThrows(IllegalStateException.class,
        () -> SagaDefinition.onManualRetry(stuck, new StuckStep(SagaState.APPROVING_ORDER, "failed")));
  }

  @Test
  void sweepIgnoresSagasBeforeDeadline() {
    receive(new OrderPlaced(order, LINES));

    assertEquals(0L, sweeper.sweepOnce().block(TIMEOUT));
    assertEquals(0, view().attempts());
  }

  @Test
  void sweeperLoopProcessesExpiredSagaUntilStopped() throws InterruptedException {
    receive(new OrderPlaced(order, LINES));
    expire();
    SagaTimeoutSweeper loop = new SagaTimeoutSweeper(store, orchestrator, transactionManager,
        new OrchestratorProperties(Duration.ofMinutes(1), 2, true, Duration.ofMillis(20), 10), attentionMonitor);

    loop.start();
    loop.start();
    assertTrue(loop.isRunning());
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (view().attempts() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    CountDownLatch stopped = new CountDownLatch(1);
    loop.stop(stopped::countDown);
    assertTrue(stopped.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
    loop.stop();

    assertEquals(false, loop.isRunning());
    assertEquals(1, view().attempts());
  }

  @Test
  void definitionRejectsTimeoutForFinishedSaga() {
    SagaInstance finished = new SagaInstance(saga, order, SagaState.COMPLETED, null, null, 0);

    assertThrows(IllegalStateException.class, () -> SagaDefinition.onRetriesExhausted(finished));
    assertTrue(SagaDefinition.onReply(finished, new OrderApproved(order)).isEmpty());
  }

  @Test
  void sagaApiReturnsStateAndHistory() {
    receive(new OrderPlaced(order, LINES));

    web.get().uri("/sagas/{id}", saga).exchange().expectStatus().isOk()
        .expectBody()
        .jsonPath("$.sagaId").isEqualTo(saga.toString())
        .jsonPath("$.orderId").isEqualTo(order.toString())
        .jsonPath("$.state").isEqualTo("RESERVING_STOCK")
        .jsonPath("$.steps[0].trigger").isEqualTo("OrderPlaced")
        .jsonPath("$.steps[0].toState").isEqualTo("RESERVING_STOCK");
    web.get().uri("/sagas/not-a-uuid").exchange().expectStatus().isBadRequest();
    web.get().uri("/sagas/{id}", UUID.randomUUID()).exchange().expectStatus().isNotFound();
  }

  @Test
  void orderEventTravelsThroughKafkaAndCommandIsPublished() {
    kafka.send(SagaTopics.ORDER_EVENTS, saga.toString(),
        codec.toJson(codec.encode(saga, new OrderPlaced(order, LINES)))).join();

    SagaMessage command = read(SagaTopics.STOCK_COMMANDS);

    ReserveStock reserve = assertInstanceOf(ReserveStock.class, command);
    assertEquals(order, reserve.orderId());
    assertNotEquals(SagaState.COMPLETED, state());
  }

  private void stuckInCancelling() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockRejected(order, "Insufficient stock"));
    expireAndSweep();
    expireAndSweep();
    expireAndSweep();
    assertEquals(SagaState.FAILED_NEEDS_ATTENTION, state());
  }

  private void stuckInReleasing() {
    receive(new OrderPlaced(order, LINES));
    receive(new StockReserved(order, PRICED));
    receive(new OrderApprovalFailed(order, "Price changed"));
    expireAndSweep();
    expireAndSweep();
    expireAndSweep();
    assertEquals(SagaState.FAILED_NEEDS_ATTENTION, state());
  }

  private WebTestClient.ResponseSpec retry() {
    return web.post().uri("/sagas/{id}/retry", saga).exchange();
  }

  private static int occurrences(String text, String value) {
    return text.split(Pattern.quote(value), -1).length - 1;
  }

  private void receive(SagaMessage message) {
    handle(codec.toJson(codec.encode(saga, message)));
  }

  private void handle(String value) {
    handler.handle(new ConsumerRecord<>("test.saga", 0, 0L, saga.toString(), value), orchestrator::handle)
        .block(TIMEOUT);
  }

  private void inTransaction(UUID sagaId, SagaMessage message) {
    TransactionalOperator.create(transactionManager)
        .transactional(orchestrator.handle(codec.encode(sagaId, message), message))
        .block(TIMEOUT);
  }

  private void expire() {
    database.sql("UPDATE saga_instance SET step_deadline = now() - interval '1 second' WHERE saga_id = :saga")
        .bind("saga", saga).then().block(TIMEOUT);
  }

  private void expireAndSweep() {
    expire();
    assertEquals(1L, sweeper.sweepOnce().block(TIMEOUT));
  }

  private SagaResponse view() {
    return store.find(saga).block(TIMEOUT);
  }

  private SagaState state() {
    return view().state();
  }

  private List<SagaMessage> commands() {
    return database.sql("SELECT envelope FROM outbox WHERE saga_id = :saga ORDER BY id")
        .bind("saga", saga)
        .map(row -> codec.decode(codec.fromJson(row.get("envelope", String.class))))
        .all().collectList().block(TIMEOUT);
  }

  private SagaMessage lastCommand() {
    List<SagaMessage> commands = commands();
    return commands.get(commands.size() - 1);
  }

  private long commandCount(Class<? extends SagaMessage> type) {
    return commands().stream().filter(type::isInstance).count();
  }

  private SagaMessage read(String topic) {
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
    throw new AssertionError("No message on " + topic);
  }
}
