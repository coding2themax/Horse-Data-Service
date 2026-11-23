package com.coding2themax.horse.stock.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.coding2themax.horse.saga.contracts.SagaTopics;
import com.coding2themax.horse.saga.messaging.SagaMessageHandler;
import com.coding2themax.horse.stock.application.StockReservationService;

import reactor.core.publisher.Mono;

@Component
public class StockCommandListener {

  private final SagaMessageHandler handler;
  private final StockReservationService reservations;

  public StockCommandListener(SagaMessageHandler handler, StockReservationService reservations) {
    this.handler = handler;
    this.reservations = reservations;
  }

  @KafkaListener(topics = SagaTopics.STOCK_COMMANDS)
  public Mono<Void> onCommand(ConsumerRecord<String, String> record) {
    return handler.handle(record, reservations::handle);
  }
}
