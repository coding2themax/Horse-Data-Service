package com.coding2themax.horse.saga.messaging;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("horse.saga.messaging")
public record SagaMessagingProperties(String consumerName, @DefaultValue Relay relay, @DefaultValue Retry retry) {

  public record Relay(@DefaultValue("true") boolean enabled, @DefaultValue("500ms") Duration pollInterval,
      @DefaultValue("100") int batchSize, @DefaultValue("10s") Duration sendTimeout) {
  }

  public record Retry(@DefaultValue("3") long attempts, @DefaultValue("1s") Duration backoff) {
  }
}
