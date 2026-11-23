package com.coding2themax.horse.saga.messaging;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Publishes committed outbox rows with at-least-once delivery. Row locks let
 * several service instances relay without selecting the same message.
 */
public class OutboxRelay implements SmartLifecycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(OutboxRelay.class);

  private static final String NEXT_BATCH = """
      SELECT id, saga_id, topic, envelope FROM outbox
      WHERE published_at IS NULL
      ORDER BY id
      LIMIT :limit
      FOR UPDATE SKIP LOCKED
      """;
  private static final String MARK_PUBLISHED = "UPDATE outbox SET published_at = now() WHERE id = :id";

  private final DatabaseClient database;
  private final TransactionalOperator transaction;
  private final OutboxPublisher publisher;
  private final SagaMessagingProperties.Relay settings;
  private volatile boolean running;
  private Sinks.Empty<Void> stopRequested;
  private Sinks.Empty<Void> stopped;

  public OutboxRelay(DatabaseClient database, ReactiveTransactionManager transactionManager,
      OutboxPublisher publisher, SagaMessagingProperties.Relay settings) {
    this.database = database;
    this.transaction = TransactionalOperator.create(transactionManager);
    this.publisher = publisher;
    this.settings = settings;
  }

  /** Publishes one batch in order, stopping at the first send failure. Emits the count sent. */
  public Mono<Integer> relayOnce() {
    Mono<Integer> work = nextBatch().flatMap(batch -> Flux.fromIterable(batch)
        .concatMap(this::publishAndMark, 1)
        .takeWhile(Boolean::booleanValue)
        .count()
        .map(Long::intValue));
    return transaction.transactional(work);
  }

  private Mono<List<OutboxRow>> nextBatch() {
    return database.sql(NEXT_BATCH)
        .bind("limit", settings.batchSize())
        .map(row -> new OutboxRow(
            row.get("id", Long.class),
            row.get("saga_id", UUID.class),
            row.get("topic", String.class),
            row.get("envelope", String.class)))
        .all()
        .collectList();
  }

  private Mono<Boolean> publishAndMark(OutboxRow row) {
    return publisher.publish(row.topic(), row.sagaId().toString(), row.envelope())
        .timeout(settings.sendTimeout())
        .thenReturn(true)
        .onErrorResume(error -> {
          LOGGER.warn("Could not publish outbox message {}; it will be retried", row.id(), error);
          return Mono.just(false);
        })
        .flatMap(sent -> sent
            ? database.sql(MARK_PUBLISHED).bind("id", row.id()).fetch().rowsUpdated().thenReturn(true)
            : Mono.just(false));
  }

  @Override
  public synchronized void start() {
    if (!settings.enabled() || running) {
      return;
    }
    Sinks.Empty<Void> finished = Sinks.empty();
    stopRequested = Sinks.empty();
    stopped = finished;
    running = true;
    Flux.interval(settings.pollInterval())
        .takeUntilOther(stopRequested.asMono())
        .onBackpressureDrop()
        .concatMap(tick -> relayOnce().onErrorResume(error -> {
          LOGGER.warn("Outbox relay cycle failed", error);
          return Mono.empty();
        }), 1)
        .doFinally(signal -> {
          running = false;
          finished.tryEmitEmpty();
        })
        .subscribe();
  }

  @Override
  public void stop() {
    stop(() -> {
    });
  }

  /** Stops polling and lets an in-flight transaction finish before signalling completion. */
  @Override
  public synchronized void stop(Runnable callback) {
    if (!running) {
      callback.run();
      return;
    }
    stopped.asMono().doFinally(signal -> callback.run()).subscribe();
    stopRequested.tryEmitEmpty();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private record OutboxRow(Long id, UUID sagaId, String topic, String envelope) {
  }
}
