package com.coding2themax.horse.saga.messaging;

import org.springframework.kafka.core.KafkaTemplate;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Publishes without blocking the caller. KafkaProducer.send can block while it
 * waits for topic metadata, so it must never run on an R2DBC event-loop thread.
 * Completion moves off Kafka's I/O thread before downstream work continues.
 */
public class KafkaOutboxPublisher implements OutboxPublisher {

  private final KafkaTemplate<String, String> template;

  public KafkaOutboxPublisher(KafkaTemplate<String, String> template) {
    this.template = template;
  }

  @Override
  public Mono<Void> publish(String topic, String key, String value) {
    return Mono.fromFuture(() -> template.send(topic, key, value))
        .subscribeOn(Schedulers.boundedElastic())
        .publishOn(Schedulers.parallel())
        .then();
  }
}
