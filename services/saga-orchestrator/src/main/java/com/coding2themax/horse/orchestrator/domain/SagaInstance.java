package com.coding2themax.horse.orchestrator.domain;

import java.util.UUID;

public record SagaInstance(UUID sagaId, UUID orderId, SagaState state, String currentCommand, String failureReason,
    int attempts) {
}
