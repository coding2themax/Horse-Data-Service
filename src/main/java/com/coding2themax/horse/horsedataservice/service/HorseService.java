package com.coding2themax.horse.horsedataservice.service;

import com.coding2themax.horse.horsedataservice.model.Horse;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

//create an interface called HorseService
public interface HorseService {
  Mono<Horse> createHorse(Horse horse);

  Mono<Horse> updateHorse(String id, Horse horse);

  Mono<Horse> getHorseById(String id);

  Flux<Horse> getAllHorses();

  Flux<Horse> getHorsesByStatus(String status);

  Mono<Horse> deleteHorse(String id);

}
