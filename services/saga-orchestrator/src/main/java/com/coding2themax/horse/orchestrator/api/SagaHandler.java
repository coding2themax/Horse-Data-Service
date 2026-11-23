package com.coding2themax.horse.orchestrator.api;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;

import reactor.core.publisher.Mono;

@Component
public class SagaHandler {

  private final SagaStore store;

  public SagaHandler(SagaStore store) {
    this.store = store;
  }

  public Mono<ServerResponse> get(ServerRequest request) {
    return Mono.fromCallable(() -> sagaId(request))
        .flatMap(store::find)
        .flatMap(saga -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(saga))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  private static UUID sagaId(ServerRequest request) {
    try {
      return UUID.fromString(request.pathVariable("id"));
    } catch (IllegalArgumentException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Saga id must be a UUID");
    }
  }
}
