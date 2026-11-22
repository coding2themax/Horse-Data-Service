package com.coding2themax.horse.horsedataservice.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;

public interface HorseGoodRepository extends ReactiveCrudRepository<HorseGoods, Integer> {

}
