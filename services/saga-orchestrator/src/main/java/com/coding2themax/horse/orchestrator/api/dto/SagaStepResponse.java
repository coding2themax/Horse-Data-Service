package com.coding2themax.horse.orchestrator.api.dto;

import java.time.OffsetDateTime;

public record SagaStepResponse(String fromState, String toState, String trigger, String detail,
    OffsetDateTime occurredAt) {
}
