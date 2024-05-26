package com.coding2themax.horse.horsedataservice.handlers;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.coding2themax.horse.horsedataservice.model.Horse;
import com.coding2themax.horse.horsedataservice.service.HorseService;

import reactor.core.publisher.Mono;

@Component
public class HorseHandler {

  private final HorseService horseService;

  public HorseHandler(HorseService horseService) {
    this.horseService = horseService;
  }

  public Mono<ServerResponse> getAllHorses(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(horseService.getAllHorses(), Horse.class);
  }
}
