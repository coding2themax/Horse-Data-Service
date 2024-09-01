package com.coding2themax.horse.horsedataservice.service;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

public interface HorseGoodService {
  Mono<HorseGoods> addHorseGood(HorseGoods horseGood);

  Mono<HorseGoods> updateHorseGood(HorseGoods horseGood);

  Mono<HorseGoods> deleteHorseGood(String horseGoodId);

  Mono<HorseGoods> getHorseGoodById(String horseGoodId);

  Flux<HorseGoods> getAllHorseGoods();

}
