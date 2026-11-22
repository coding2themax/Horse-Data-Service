package com.coding2themax.horse.horsedataservice.handlers;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;
import com.coding2themax.horse.horsedataservice.service.HorseGoodService;

import reactor.core.publisher.Mono;

@Component
public class HorseGoodHandler {

  private final HorseGoodService horseGoodService;

  public HorseGoodHandler(HorseGoodService horseGoodService) {
    this.horseGoodService = horseGoodService;
  }

  // Add the handler methods here
  public Mono<ServerResponse> getAllHorses(ServerRequest request) {
    return horseGoodService.getAllHorseGoods()
        .collectList()
        .flatMap(horses -> ServerResponse.ok().bodyValue(horses))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  public Mono<ServerResponse> getHorseGoodsById(ServerRequest request) {
    String id = request.pathVariable("id");
    return horseGoodService.getHorseGoodById(id)
        .flatMap(horse -> ServerResponse.ok().bodyValue(horse))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  public Mono<ServerResponse> deleteHorseGood(ServerRequest request) {
    String id = request.pathVariable("id");
    return horseGoodService.deleteHorseGood(id)
        .flatMap(horse -> ServerResponse.ok().bodyValue(horse))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  public Mono<ServerResponse> createHorseGood(ServerRequest request) {
    return request.bodyToMono(HorseGoods.class)
        .flatMap(horseGood -> horseGoodService.addHorseGood(horseGood))
        .flatMap(horseGood -> ServerResponse.ok().bodyValue(horseGood))
        .switchIfEmpty(ServerResponse.badRequest().build());
  }

  public Mono<ServerResponse> updateHorseGood(ServerRequest request) {
    return request.bodyToMono(HorseGoods.class)
        .flatMap(horseGood -> horseGoodService.updateHorseGood(horseGood))
        .flatMap(horseGood -> ServerResponse.ok().bodyValue(horseGood))
        .switchIfEmpty(ServerResponse.badRequest().build());
  }

}
