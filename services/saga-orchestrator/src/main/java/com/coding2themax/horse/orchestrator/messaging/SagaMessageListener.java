package com.coding2themax.horse.orchestrator.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.coding2themax.horse.orchestrator.application.SagaOrchestrator;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;

import reactor.core.publisher.Mono;

@Component
public class SagaMessageListener {

  private final SagaMessageHandler handler;
  private final SagaOrchestrator orchestrator;

  public SagaMessageListener(SagaMessageHandler handler, SagaOrchestrator orchestrator) {
    this.handler = handler;
    this.orchestrator = orchestrator;
  }

  @KafkaListener(topics = { SagaTopics.ORDER_EVENTS, SagaTopics.STOCK_REPLIES, SagaTopics.ORDER_REPLIES })
  public Mono<Void> onMessage(ConsumerRecord<String, String> record) {
    return handler.handle(record, orchestrator::handle);
  }
}
