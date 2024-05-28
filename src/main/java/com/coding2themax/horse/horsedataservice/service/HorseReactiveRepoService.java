package com.coding2themax.horse.horsedataservice.service;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.coding2themax.horse.horsedataservice.model.Horse;
import com.coding2themax.horse.horsedataservice.repository.HorseRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Constructs a new HorseReactiveRepoService with the specified HorseRepository.
 *
 * @param horseRepository the HorseRepository to be used by the service
 */
@Service
public class HorseReactiveRepoService implements HorseService {

  @Autowired
  private HorseRepository horseRepository;

  public HorseReactiveRepoService(HorseRepository horseRepository) {
    this.horseRepository = horseRepository;
  }

  @Override
  public Mono<Horse> createHorse(Horse horse) {
    return saveHorseOrUpdate(horse);
  }

  @Override
  public Mono<Horse> updateHorse(String id, Horse horse) {
    horse.setId(Integer.valueOf(id));
    return saveHorseOrUpdate(horse);
  }

  @Override
  public Mono<Horse> getHorseById(String id) {
    return horseRepository.findById(Integer.valueOf(id));
  }

  @Override
  public Flux<Horse> getAllHorses() {
    return horseRepository.findAll();
  }

  private Mono<Horse> saveHorseOrUpdate(Horse horse) {
    Integer id = Optional.ofNullable(horse.getId()).orElse(0); // Add null check and provide a non-null value
    return this.horseRepository.findById(id).flatMap(h -> {
      h.setName(horse.getName());
      h.setBreed(horse.getBreed());
      h.setColor(horse.getColor());
      h.setAge(horse.getAge());
      h.setCoatPattern(horse.getCoatPattern());
      h.setTrainingLevel(horse.getTrainingLevel());
      return this.horseRepository.save(h);
    }).switchIfEmpty(this.horseRepository.save(horse));
  }
}
