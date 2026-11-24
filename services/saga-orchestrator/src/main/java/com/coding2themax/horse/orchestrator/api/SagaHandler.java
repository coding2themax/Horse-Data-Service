package com.coding2themax.horse.orchestrator.api;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.orchestrator.api.dto.SagaAttentionResponse;
import com.coding2themax.horse.orchestrator.application.SagaRecoveryService;
import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;

import reactor.core.publisher.Mono;

@Component
public class SagaHandler {

  private static final int ATTENTION_LIST_LIMIT = 200;

  private final SagaStore store;
  private final SagaRecoveryService recovery;

  public SagaHandler(SagaStore store, SagaRecoveryService recovery) {
    this.store = store;
    this.recovery = recovery;
  }

  public Mono<ServerResponse> get(ServerRequest request) {
    return Mono.fromCallable(() -> sagaId(request))
        .flatMap(store::find)
        .flatMap(saga -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(saga))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  public Mono<ServerResponse> needingAttention(ServerRequest request) {
    return store.countNeedingAttention()
        .zipWith(store.findNeedingAttention(ATTENTION_LIST_LIMIT).collectList(), SagaAttentionResponse::new)
        .flatMap(sagas -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(sagas));
  }

  public Mono<ServerResponse> retry(ServerRequest request) {
    return Mono.fromCallable(() -> sagaId(request))
        .flatMap(recovery::retry)
        .flatMap(saga -> ServerResponse.accepted().contentType(MediaType.APPLICATION_JSON).bodyValue(saga));
  }

  private static UUID sagaId(ServerRequest request) {
    try {
      return UUID.fromString(request.pathVariable("id"));
    } catch (IllegalArgumentException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Saga id must be a UUID");
    }
  }
}
