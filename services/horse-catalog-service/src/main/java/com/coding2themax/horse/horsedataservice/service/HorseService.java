package com.coding2themax.horse.horsedataservice.service;

import com.coding2themax.horse.horsedataservice.model.Horse;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

//create an interface called HorseService
public interface HorseService {
  Mono<Horse> createHorse(Horse horse);

  Mono<Horse> updateHorse(@Parameter(in = ParameterIn.PATH) String id, Horse horse);

  Mono<Horse> getHorseById(@Parameter(in = ParameterIn.PATH) String id);

  Flux<Horse> getAllHorses();

  Flux<Horse> getHorsesByStatus(String status);

  Mono<Horse> deleteHorse(@Parameter(in = ParameterIn.PATH) String id);

}
