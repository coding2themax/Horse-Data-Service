package com.coding2themax.horse.saga.messaging;

import org.springframework.kafka.core.KafkaTemplate;

import reactor.core.publisher.Mono;

/** Publishes without blocking by adapting the Kafka send future. */
public class KafkaOutboxPublisher implements OutboxPublisher {

  private final KafkaTemplate<String, String> template;

  public KafkaOutboxPublisher(KafkaTemplate<String, String> template) {
    this.template = template;
  }

  @Override
  public Mono<Void> publish(String topic, String key, String value) {
    return Mono.fromFuture(() -> template.send(topic, key, value)).then();
  }
}
