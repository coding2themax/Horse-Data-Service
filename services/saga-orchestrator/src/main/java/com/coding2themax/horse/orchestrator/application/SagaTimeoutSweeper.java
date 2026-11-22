package com.coding2themax.horse.orchestrator.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/** Finds sagas past their step deadline. Each saga has its own transaction. */
@Component
public class SagaTimeoutSweeper implements SmartLifecycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

  private final SagaStore store;
  private final SagaOrchestrator orchestrator;
  private final TransactionalOperator transaction;
  private final OrchestratorProperties properties;
  private volatile boolean running;
  private Sinks.Empty<Void> stopRequested;
  private Sinks.Empty<Void> stopped;

  public SagaTimeoutSweeper(SagaStore store, SagaOrchestrator orchestrator,
      ReactiveTransactionManager transactionManager, OrchestratorProperties properties) {
    this.store = store;
    this.orchestrator = orchestrator;
    this.transaction = TransactionalOperator.create(transactionManager);
    this.properties = properties;
  }

  /** Emits the number of expired sagas processed. One failure does not stop the others. */
  public Mono<Long> sweepOnce() {
    return store.findExpired(properties.sweepBatchSize())
        .collectList()
        .flatMapMany(Flux::fromIterable)
        .concatMap(sagaId -> transaction.transactional(orchestrator.expire(sagaId))
            .thenReturn(sagaId)
            .onErrorResume(error -> {
              LOGGER.error("Could not process timeout for saga {}", sagaId, error);
              return Mono.empty();
            }))
        .count();
  }

  @Override
  public synchronized void start() {
    if (!properties.sweepEnabled() || running) {
      return;
    }
    Sinks.Empty<Void> finished = Sinks.empty();
    stopRequested = Sinks.empty();
    stopped = finished;
    running = true;
    Flux.interval(properties.sweepInterval())
        .takeUntilOther(stopRequested.asMono())
        .onBackpressureDrop()
        .concatMap(tick -> sweepOnce().onErrorResume(error -> {
          LOGGER.warn("Saga timeout sweep failed", error);
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
}
