package com.coding2themax.horse.saga.contracts;

/** Kafka topics used by the store order saga. */
public final class SagaTopics {

  public static final String ORDER_EVENTS = "horse.order.events";
  public static final String ORDER_COMMANDS = "horse.order.commands";
  public static final String ORDER_REPLIES = "horse.order.replies";
  public static final String STOCK_COMMANDS = "horse.stock.commands";
  public static final String STOCK_REPLIES = "horse.stock.replies";

  private static final String DEAD_LETTER_SUFFIX = ".dlt";

  private SagaTopics() {
  }

  public static String deadLetter(String topic) {
    return topic + DEAD_LETTER_SUFFIX;
  }
}
