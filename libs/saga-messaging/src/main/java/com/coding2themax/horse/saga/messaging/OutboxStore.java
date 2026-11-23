package com.coding2themax.horse.saga.messaging;

import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.SagaMessage;

import reactor.core.publisher.Mono;

/** Stores outgoing messages in the caller's database transaction. */
public class OutboxStore {

  private static final String INSERT = """
      INSERT INTO outbox (message_id, saga_id, topic, envelope)
      VALUES (:messageId, :sagaId, :topic, :envelope)
      """;

  private final DatabaseClient database;
  private final SagaMessageCodec codec;
  private final TransactionalOperator requireTransaction;

  public OutboxStore(DatabaseClient database, ReactiveTransactionManager transactionManager, SagaMessageCodec codec) {
    this.database = database;
    this.codec = codec;
    this.requireTransaction = TransactionalOperator.create(transactionManager,
        new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_MANDATORY));
  }

  /** Fails if no transaction is active, so state and message always commit together. */
  public Mono<MessageEnvelope> append(String topic, UUID sagaId, SagaMessage message) {
    Mono<MessageEnvelope> insert = Mono.fromSupplier(() -> codec.encode(sagaId, message))
        .flatMap(envelope -> database.sql(INSERT)
            .bind("messageId", envelope.messageId())
            .bind("sagaId", envelope.sagaId())
            .bind("topic", topic)
            .bind("envelope", codec.toJson(envelope))
            .fetch()
            .rowsUpdated()
            .thenReturn(envelope));
    return requireTransaction.transactional(insert);
  }
}
