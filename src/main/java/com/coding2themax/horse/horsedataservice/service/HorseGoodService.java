package com.coding2themax.horse.horsedataservice.service;

import java.util.List;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

public interface HorseGoodService {
  Mono<Void> addHorseGood(HorseGoods horseGood);

  Mono<Void> updateHorseGood(HorseGoods horseGood);

  Mono<Void> deleteHorseGood(String horseGoodId);

  Mono<HorseGoods> getHorseGoodById(String horseGoodId);

  Flux<HorseGoods> getAllHorseGoods();
}
