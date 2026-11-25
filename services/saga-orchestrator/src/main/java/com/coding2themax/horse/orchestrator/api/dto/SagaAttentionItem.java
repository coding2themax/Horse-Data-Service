package com.coding2themax.horse.orchestrator.api.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record SagaAttentionItem(UUID sagaId, UUID orderId, String failureReason, OffsetDateTime updatedAt) {
}
