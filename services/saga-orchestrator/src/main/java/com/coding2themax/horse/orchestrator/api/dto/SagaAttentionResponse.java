package com.coding2themax.horse.orchestrator.api.dto;

import java.util.List;

/** Total counts every stuck saga. The list holds the oldest ones, up to the API limit. */
public record SagaAttentionResponse(long total, List<SagaAttentionItem> sagas) {
}
