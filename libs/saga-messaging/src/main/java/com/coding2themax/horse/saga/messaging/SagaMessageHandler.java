package com.coding2themax.horse.saga.messaging;

import java.time.Duration;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaTopics;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Processes a Kafka record for an async listener. Retries and dead-lettering
 * happen here because Spring Kafka 3.2 acknowledges failed Mono listeners
 * without calling the container error handler. The returned Mono completes
 * only when the record is processed or safely stored on its dead-letter topic.
 */
public class SagaMessageHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SagaMessageHandler.class);
  private static final Duration MAX_DEAD_LETTER_BACKOFF = Duration.ofSeconds(30);

  private final InboxProcessor inbox;
  private final SagaMessageCodec codec;
  private final OutboxPublisher publisher;
  private final SagaMessagingProperties.Retry retry;

  public SagaMessageHandler(InboxProcessor inbox, SagaMessageCodec codec, OutboxPublisher publisher,
      SagaMessagingProperties.Retry retry) {
    this.inbox = inbox;
    this.codec = codec;
    this.publisher = publisher;
    this.retry = retry;
  }

  public Mono<Void> handle(ConsumerRecord<String, String> record,
      BiFunction<MessageEnvelope, SagaMessage, Mono<Void>> handler) {
    return Mono.fromCallable(() -> parse(record))
        .flatMap(parsed -> inbox.processOnce(parsed.envelope(),
            envelope -> handler.apply(envelope, parsed.message()))
            .retryWhen(Retry.fixedDelay(retry.attempts(), retry.backoff())))
        .then()
        .onErrorResume(error -> deadLetter(record, error));
  }

  private Parsed parse(ConsumerRecord<String, String> record) {
    MessageEnvelope envelope = codec.fromJson(record.value());
    return new Parsed(envelope, codec.decode(envelope));
  }

  /** Keeps retrying so a broker outage cannot acknowledge and lose the record. */
  private Mono<Void> deadLetter(ConsumerRecord<String, String> record, Throwable error) {
    String topic = SagaTopics.deadLetter(record.topic());
    LOGGER.error("Sending record {}-{}@{} to {}", record.topic(), record.partition(), record.offset(), topic, error);
    return Mono.defer(() -> publisher.publish(topic, record.key(), record.value()))
        .retryWhen(Retry.backoff(Long.MAX_VALUE, retry.backoff()).maxBackoff(MAX_DEAD_LETTER_BACKOFF));
  }

  private record Parsed(MessageEnvelope envelope, SagaMessage message) {
  }
}
