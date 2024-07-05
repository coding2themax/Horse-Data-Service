package com.coding2themax.horse.horsedataservice.repository;

import com.coding2themax.horse.horsedataservice.model.HorseStatus;

import reactor.core.publisher.Flux;

public interface HorseStatusRepository {

  Flux<HorseStatus> findAll();

}
