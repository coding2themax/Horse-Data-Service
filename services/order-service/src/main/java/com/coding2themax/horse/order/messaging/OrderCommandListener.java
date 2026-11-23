package com.coding2themax.horse.order.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.coding2themax.horse.order.application.OrderCommandService;
import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;

import reactor.core.publisher.Mono;

@Component
public class OrderCommandListener {

  private final SagaMessageHandler handler;
  private final OrderCommandService commands;

  public OrderCommandListener(SagaMessageHandler handler, OrderCommandService commands) {
    this.handler = handler;
    this.commands = commands;
  }

  @KafkaListener(topics = SagaTopics.ORDER_COMMANDS)
  public Mono<Void> onCommand(ConsumerRecord<String, String> record) {
    return handler.handle(record, commands::handle);
  }
}
