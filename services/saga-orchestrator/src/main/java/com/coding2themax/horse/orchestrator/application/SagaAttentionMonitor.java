package com.coding2themax.horse.orchestrator.application;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Mono;

/**
 * Raises one error log per stuck saga and publishes how many are stuck. Alerts
 * are claimed in the database, so a restart or second instance does not repeat them.
 */
@Component
public class SagaAttentionMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SagaAttentionMonitor.class);

  private final SagaStore store;
  private final OrchestratorProperties properties;
  private final AtomicLong needingAttention = new AtomicLong();

  public SagaAttentionMonitor(SagaStore store, OrchestratorProperties properties, MeterRegistry registry) {
    this.store = store;
    this.properties = properties;
    Gauge.builder("saga.needs.attention", needingAttention, AtomicLong::get)
        .description("Sagas in FAILED_NEEDS_ATTENTION")
        .register(registry);
  }

  /** Emits the number of sagas that currently need attention. */
  public Mono<Long> checkOnce() {
    return store.claimUnalerted(properties.sweepBatchSize())
        .doOnNext(saga -> LOGGER.error("Saga needs attention: sagaId={} orderId={} reason={}", saga.sagaId(),
            saga.orderId(), saga.failureReason()))
        .then(store.countNeedingAttention())
        .doOnNext(needingAttention::set);
  }
}
