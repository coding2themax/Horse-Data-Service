package com.coding2themax.horse.saga.messaging;

import java.util.function.Function;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;

import reactor.core.publisher.Mono;

/** Runs a handler at most once per message, in the same transaction as the inbox record. */
public class InboxProcessor {

  private static final String CLAIM = """
      INSERT INTO inbox (consumer, message_id) VALUES (:consumer, :messageId)
      ON CONFLICT DO NOTHING
      """;

  private final DatabaseClient database;
  private final TransactionalOperator transaction;
  private final String consumer;

  public InboxProcessor(DatabaseClient database, ReactiveTransactionManager transactionManager, String consumer) {
    this.database = database;
    this.transaction = TransactionalOperator.create(transactionManager);
    this.consumer = consumer;
  }

  /** Emits true if the handler ran, or false for a duplicate. A failure rolls back the claim. */
  public Mono<Boolean> processOnce(MessageEnvelope envelope, Function<MessageEnvelope, Mono<Void>> handler) {
    Mono<Boolean> work = database.sql(CLAIM)
        .bind("consumer", consumer)
        .bind("messageId", envelope.messageId())
        .fetch()
        .rowsUpdated()
        .flatMap(claimed -> claimed == 0
            ? Mono.just(false)
            : handler.apply(envelope).thenReturn(true));
    return transaction.transactional(work);
  }
}
