package com.coding2themax.horse.orchestrator.application;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("horse.saga.orchestrator")
public record OrchestratorProperties(@DefaultValue("30s") Duration stepTimeout, @DefaultValue("3") int maxRetries,
    @DefaultValue("true") boolean sweepEnabled, @DefaultValue("5s") Duration sweepInterval,
    @DefaultValue("100") int sweepBatchSize) {
}
