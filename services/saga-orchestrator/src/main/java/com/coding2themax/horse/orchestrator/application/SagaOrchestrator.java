package com.coding2themax.horse.orchestrator.application;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.coding2themax.horse.orchestrator.domain.SagaDefinition;
import com.coding2themax.horse.orchestrator.domain.SagaInstance;
import com.coding2themax.horse.orchestrator.domain.SagaState;
import com.coding2themax.horse.orchestrator.domain.Transition;
import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;
import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ApproveOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.CancelOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderPlaced;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReleaseStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReserveStock;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.OutboxStore;
import com.coding2themax.horse.saga.messaging.SagaMessageCodec;

import reactor.core.publisher.Mono;

/**
 * Advances sagas. Each step runs in one transaction, so the state change, step
 * log, and next command commit together and survive a restart.
 */
@Service
public class SagaOrchestrator {

  private final SagaStore store;
  private final OutboxStore outbox;
  private final SagaMessageCodec codec;
  private final OrchestratorProperties properties;

  public SagaOrchestrator(SagaStore store, OutboxStore outbox, SagaMessageCodec codec,
      OrchestratorProperties properties) {
    this.store = store;
    this.outbox = outbox;
    this.codec = codec;
    this.properties = properties;
  }

  public Mono<Void> handle(MessageEnvelope envelope, SagaMessage message) {
    return message instanceof OrderPlaced placed
        ? start(envelope.sagaId(), placed)
        : onReply(envelope, message);
  }

  /** Retries the current step or moves on. Must run in a transaction. */
  public Mono<Void> expire(UUID sagaId) {
    return store.lockExpired(sagaId).flatMap(saga -> saga.attempts() < properties.maxRetries()
        ? retry(saga)
        : apply(saga, SagaDefinition.onRetriesExhausted(saga), "TIMEOUT"));
  }

  private Mono<Void> start(UUID sagaId, OrderPlaced placed) {
    ReserveStock command = new ReserveStock(placed.orderId(), placed.lines());
    return store.insert(sagaId, placed.orderId(), serialize(sagaId, command), timeoutSeconds())
        .flatMap(inserted -> inserted
            ? store.logStep(sagaId, null, SagaState.RESERVING_STOCK, "OrderPlaced", null)
                .then(send(sagaId, command))
            : Mono.empty());
  }

  private Mono<Void> onReply(MessageEnvelope envelope, SagaMessage reply) {
    return store.lock(envelope.sagaId())
        .switchIfEmpty(Mono.error(() -> new IllegalStateException("Unknown saga " + envelope.sagaId())))
        .flatMap(saga -> {
          if (!saga.orderId().equals(reply.orderId())) {
            return Mono.error(new IllegalStateException("Reply order does not match saga " + saga.sagaId()));
          }
          return SagaDefinition.onReply(saga, reply)
              .map(transition -> apply(saga, transition, envelope.type()))
              .orElseGet(() -> store.logStep(saga.sagaId(), saga.state(), saga.state(), envelope.type(),
                  "Ignored late or duplicate reply"));
        });
  }

  private Mono<Void> apply(SagaInstance saga, Transition transition, String trigger) {
    String command = transition.command() == null ? null : serialize(saga.sagaId(), transition.command());
    Mono<Void> update = store.applyTransition(saga.sagaId(), transition.next(), command, transition.failureReason(),
        timeoutSeconds())
        .then(store.logStep(saga.sagaId(), saga.state(), transition.next(), trigger, transition.failureReason()));
    return transition.command() == null ? update : update.then(send(saga.sagaId(), transition.command()));
  }

  /** Sends with a new message id, because the inbox would drop a repeated id without replying. */
  private Mono<Void> retry(SagaInstance saga) {
    SagaMessage command = codec.decode(codec.fromJson(saga.currentCommand()));
    return store.recordRetry(saga.sagaId(), timeoutSeconds())
        .then(store.logStep(saga.sagaId(), saga.state(), saga.state(), "TIMEOUT_RETRY",
            "Retry " + (saga.attempts() + 1) + " of " + properties.maxRetries()))
        .then(send(saga.sagaId(), command));
  }

  private Mono<Void> send(UUID sagaId, SagaMessage command) {
    return outbox.append(topicFor(command), sagaId, command).then();
  }

  private String serialize(UUID sagaId, SagaMessage command) {
    return codec.toJson(codec.encode(sagaId, command));
  }

  private double timeoutSeconds() {
    return properties.stepTimeout().toMillis() / 1000.0;
  }

  private static String topicFor(SagaMessage command) {
    return switch (command) {
      case ReserveStock c -> SagaTopics.STOCK_COMMANDS;
      case ReleaseStock c -> SagaTopics.STOCK_COMMANDS;
      case ApproveOrder c -> SagaTopics.ORDER_COMMANDS;
      case CancelOrder c -> SagaTopics.ORDER_COMMANDS;
      default -> throw new IllegalArgumentException("Not a saga command: " + command.getClass().getSimpleName());
    };
  }
}
