package com.coding2themax.horse.orchestrator.domain;

/** The compensation that stalled and the failure reason it was carrying. */
public record StuckStep(SagaState state, String reason) {
}
