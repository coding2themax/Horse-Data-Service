package com.coding2themax.horse.orchestrator.domain;

import com.coding2themax.horse.saga.contracts.SagaMessage;

/** The next saga state and the command to send, if any. */
public record Transition(SagaState next, SagaMessage command, String failureReason) {
}
