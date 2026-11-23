package com.coding2themax.horse.saga.messaging;

import reactor.core.publisher.Mono;

/** Sends one stored outbox message to the broker. */
@FunctionalInterface
public interface OutboxPublisher {

  Mono<Void> publish(String topic, String key, String value);
}
