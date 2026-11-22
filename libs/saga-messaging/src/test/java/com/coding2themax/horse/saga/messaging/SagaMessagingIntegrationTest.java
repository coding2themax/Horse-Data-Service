package com.coding2themax.horse.saga.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaTopics;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootTest(classes = SagaMessagingIntegrationTest.TestApplication.class, properties = {
    "spring.application.name=saga-messaging-test",
    "spring.flyway.locations=classpath:db/saga-messaging",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "horse.saga.messaging.relay.enabled=false",
    "horse.saga.messaging.retry.attempts=1",
    "horse.saga.messaging.retry.backoff=10ms" })
@Testcontainers
class SagaMessagingIntegrationTest {

  private static final String FAILING_TOPIC = "saga-test.failing";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:14");

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(POSTGRES.getHost(),
        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), POSTGRES.getDatabaseName()));
    registry.add("spring.r2dbc.username", POSTGRES::getUsername);
    registry.add("spring.r2dbc.password", POSTGRES::getPassword);
    registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
    registry.add("spring.flyway.user", POSTGRES::getUsername);
    registry.add("spring.flyway.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired
  private ConnectionFactory connectionFactory;
  @Autowired
  private ReactiveTransactionManager transactionManager;
  @Autowired
  private OutboxStore outbox;
  @Autowired
  private InboxProcessor inbox;
  @Autowired
  private OutboxRelay relay;
  @Autowired
  private SagaMessageCodec codec;
  @Autowired
  private KafkaTemplate<String, String> kafka;
  @Autowired
  private SagaMessageHandler handler;

  private DatabaseClient database;
  private TransactionalOperator transaction;

  @BeforeEach
  void clean() {
    database = DatabaseClient.create(connectionFactory);
    transaction = TransactionalOperator.create(transactionManager);
    database.sql("TRUNCATE outbox, inbox").then().block(TIMEOUT);
  }

  @Test
  void appendRequiresTransaction() {
    StepVerifier.create(outbox.append("topic", UUID.randomUUID(), message()))
        .expectError(IllegalTransactionStateException.class)
        .verify(TIMEOUT);
    assertEquals(0, count("SELECT count(*) FROM outbox"));
  }

  @Test
  void rollbackDiscardsOutboxMessage() {
    Mono<Void> failed = outbox.append("topic", UUID.randomUUID(), message())
        .then(Mono.error(new IllegalStateException("business failure")));
    assertThrows(IllegalStateException.class, () -> transaction.transactional(failed).block(TIMEOUT));
    assertEquals(0, count("SELECT count(*) FROM outbox"));
  }

  @Test
  void relayPublishesCommittedMessageToKafkaOnce() {
    String topic = "saga-test." + UUID.randomUUID();
    UUID saga = UUID.randomUUID();
    MessageEnvelope sent = append(topic, saga);

    assertEquals(1, relay.relayOnce().block(TIMEOUT));
    assertEquals(0, relay.relayOnce().block(TIMEOUT));
    assertEquals(0, count("SELECT count(*) FROM outbox WHERE published_at IS NULL"));

    ConsumerRecord<String, String> received = readOne(topic);
    assertEquals(saga.toString(), received.key());
    assertEquals(sent, codec.fromJson(received.value()));
  }

  @Test
  void sendFailureStopsBatchAndKeepsRemainingMessages() {
    append("topic", UUID.randomUUID());
    append("topic", UUID.randomUUID());
    append("topic", UUID.randomUUID());
    AtomicInteger attempts = new AtomicInteger();
    OutboxRelay failing = relayWith((topic, key, value) -> attempts.incrementAndGet() == 2
        ? Mono.error(new IllegalStateException("broker down"))
        : Mono.empty(), false);

    assertEquals(1, failing.relayOnce().block(TIMEOUT));
    assertEquals(2, attempts.get());
    assertEquals(2, count("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
  }

  @Test
  void concurrentRelaysDoNotSendTheSameMessage() {
    for (int i = 0; i < 20; i++) {
      append("topic", UUID.randomUUID());
    }
    Set<String> sent = ConcurrentHashMap.newKeySet();
    List<String> duplicates = new CopyOnWriteArrayList<>();
    OutboxPublisher publisher = (topic, key, value) -> Mono.delay(Duration.ofMillis(5)).then(Mono.fromRunnable(() -> {
      if (!sent.add(value)) {
        duplicates.add(value);
      }
    }));
    OutboxRelay first = relayWith(publisher, false);
    OutboxRelay second = relayWith(publisher, false);

    Flux.merge(first.relayOnce(), second.relayOnce()).then().block(TIMEOUT);
    Flux.merge(first.relayOnce(), second.relayOnce()).then().block(TIMEOUT);

    assertTrue(duplicates.isEmpty());
    assertEquals(20, sent.size());
    assertEquals(0, count("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
  }

  @Test
  void relayLoopPublishesUntilStopped() throws InterruptedException {
    Set<String> sent = ConcurrentHashMap.newKeySet();
    OutboxRelay looping = relayWith((topic, key, value) -> Mono.fromRunnable(() -> sent.add(value)), true);
    append("topic", UUID.randomUUID());

    looping.start();
    looping.start();
    assertTrue(looping.isRunning());
    Flux.interval(Duration.ofMillis(20)).filter(tick -> !sent.isEmpty()).next().block(TIMEOUT);
    CountDownLatch stopped = new CountDownLatch(1);
    looping.stop(stopped::countDown);
    assertTrue(stopped.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
    looping.stop();

    assertFalse(looping.isRunning());
    assertEquals(1, sent.size());
  }

  @Test
  void inboxRunsHandlerOncePerMessage() {
    MessageEnvelope envelope = codec.encode(UUID.randomUUID(), message());
    AtomicInteger calls = new AtomicInteger();

    assertTrue(inbox.processOnce(envelope, e -> Mono.fromRunnable(calls::incrementAndGet)).block(TIMEOUT));
    assertFalse(inbox.processOnce(envelope, e -> Mono.fromRunnable(calls::incrementAndGet)).block(TIMEOUT));

    assertEquals(1, calls.get());
  }

  @Test
  void failedHandlerCanBeRetried() {
    MessageEnvelope envelope = codec.encode(UUID.randomUUID(), message());

    assertThrows(IllegalStateException.class,
        () -> inbox.processOnce(envelope, e -> Mono.error(new IllegalStateException("failed"))).block(TIMEOUT));
    assertEquals(0, count("SELECT count(*) FROM inbox"));

    assertTrue(inbox.processOnce(envelope, e -> Mono.empty()).block(TIMEOUT));
  }

  @Test
  void failingAsyncListenerSendsMessageToDeadLetterTopic() {
    String key = UUID.randomUUID().toString();
    String value = codec.toJson(codec.encode(UUID.fromString(key), message()));
    kafka.send(FAILING_TOPIC, key, value).join();

    ConsumerRecord<String, String> dead = readOne(SagaTopics.deadLetter(FAILING_TOPIC));

    assertEquals(key, dead.key());
    assertEquals(value, dead.value());
    assertEquals(0, count("SELECT count(*) FROM inbox"));
  }

  @Test
  void malformedRecordGoesToDeadLetterWithoutCallingHandler() {
    String topic = "saga-test." + UUID.randomUUID();
    AtomicInteger calls = new AtomicInteger();

    handler.handle(new ConsumerRecord<>(topic, 0, 0L, "key", "not-json"), (envelope, message) -> {
      calls.incrementAndGet();
      return Mono.empty();
    }).block(TIMEOUT);

    assertEquals("not-json", readOne(SagaTopics.deadLetter(topic)).value());
    assertEquals(0, calls.get());
  }

  @Test
  void transientHandlerFailureIsRetried() {
    SagaMessage sent = message();
    String value = codec.toJson(codec.encode(UUID.randomUUID(), sent));
    AtomicInteger calls = new AtomicInteger();

    handler.handle(new ConsumerRecord<>("topic", 0, 0L, "key", value), (envelope, received) -> {
      assertEquals(sent, received);
      return calls.incrementAndGet() == 1 ? Mono.error(new IllegalStateException("transient")) : Mono.empty();
    }).block(TIMEOUT);

    assertEquals(2, calls.get());
    assertEquals(1, count("SELECT count(*) FROM inbox"));
  }

  @Test
  void deadLetterPublishIsRetriedUntilItSucceeds() {
    AtomicInteger attempts = new AtomicInteger();
    SagaMessageHandler flaky = new SagaMessageHandler(inbox, codec,
        (topic, key, value) -> attempts.incrementAndGet() < 3
            ? Mono.error(new IllegalStateException("broker down"))
            : Mono.empty(),
        new SagaMessagingProperties.Retry(0, Duration.ofMillis(5)));

    flaky.handle(new ConsumerRecord<>("topic", 0, 0L, "key", "not-json"), (envelope, message) -> Mono.empty())
        .block(TIMEOUT);

    assertEquals(3, attempts.get());
  }

  private MessageEnvelope append(String topic, UUID saga) {
    return transaction.transactional(outbox.append(topic, saga, new SagaMessage.OrderApproved(UUID.randomUUID())))
        .block(TIMEOUT);
  }

  private OutboxRelay relayWith(OutboxPublisher publisher, boolean enabled) {
    return new OutboxRelay(database, transactionManager, publisher,
        new SagaMessagingProperties.Relay(enabled, Duration.ofMillis(10), 100, Duration.ofSeconds(5)));
  }

  private long count(String sql) {
    return database.sql(sql).map(row -> row.get(0, Long.class)).one().block(TIMEOUT);
  }

  private static SagaMessage message() {
    return new SagaMessage.OrderCancelled(UUID.randomUUID());
  }

  private static ConsumerRecord<String, String> readOne(String topic) {
    Map<String, Object> config = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
        new StringDeserializer())) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        var records = consumer.poll(Duration.ofMillis(250));
        if (!records.isEmpty()) {
          return records.iterator().next();
        }
      }
    }
    throw new AssertionError("No message received on " + topic);
  }

  @SpringBootApplication
  static class TestApplication {

    @Bean
    NewTopic failingTopic() {
      return new NewTopic(FAILING_TOPIC, 1, (short) 1);
    }

    @Bean
    FailingListener failingListener(SagaMessageHandler handler) {
      return new FailingListener(handler);
    }
  }

  static class FailingListener {

    private final SagaMessageHandler handler;

    FailingListener(SagaMessageHandler handler) {
      this.handler = handler;
    }

    @KafkaListener(topics = FAILING_TOPIC, groupId = "failing-listener")
    Mono<Void> listen(ConsumerRecord<String, String> record) {
      return handler.handle(record, (envelope, message) -> Mono.error(new IllegalStateException("cannot process")));
    }
  }
}
