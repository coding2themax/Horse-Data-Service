package com.coding2themax.horse.horsedataservice.handlers;

import java.util.logging.Logger;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.coding2themax.horse.horsedataservice.model.Horse;
import com.coding2themax.horse.horsedataservice.service.HorseService;

import reactor.core.publisher.Mono;

@Component
public class HorseHandler {

  private static final Logger LOGGER = Logger.getLogger(HorseHandler.class.getName());
  private final HorseService horseService;

  public HorseHandler(HorseService horseService) {
    this.horseService = horseService;
  }

  public Mono<ServerResponse> getAllHorses(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(horseService.getAllHorses(), Horse.class);
  }

  public Mono<ServerResponse> getHorseById(ServerRequest request) {
    String id = request.pathVariable("id");
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(horseService.getHorseById(id), Horse.class);
  }

  public Mono<ServerResponse> deleteHorse(ServerRequest request) {
    String id = request.pathVariable("id");
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(horseService.deleteHorse(id), Horse.class);
  }

  public Mono<ServerResponse> getHorsesByStatusID(ServerRequest request) {
    String status = request.pathVariable("status");
    LOGGER.info("Status: " + status);
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(horseService.getHorsesByStatus(status), Horse.class);
  }

  public Mono<ServerResponse> createHorse(ServerRequest request) {
    Mono<Horse> horseMono = request.bodyToMono(Horse.class);
    return horseMono.flatMap(horse -> {
      Mono<Horse> createdHorseMono = horseService.createHorse(horse);
      return createdHorseMono.flatMap(createdHorse -> ServerResponse
          .ok()
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(createdHorse))
          .switchIfEmpty(ServerResponse.badRequest().build());
    });
  }
}
