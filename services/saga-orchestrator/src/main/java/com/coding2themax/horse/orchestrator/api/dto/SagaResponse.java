package com.coding2themax.horse.orchestrator.api.dto;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import com.coding2themax.horse.orchestrator.domain.SagaState;

public record SagaResponse(UUID sagaId, UUID orderId, SagaState state, String failureReason, int attempts,
    OffsetDateTime createdAt, OffsetDateTime updatedAt, List<SagaStepResponse> steps) {

  public SagaResponse withSteps(List<SagaStepResponse> sagaSteps) {
    return new SagaResponse(sagaId, orderId, state, failureReason, attempts, createdAt, updatedAt, sagaSteps);
  }
}
