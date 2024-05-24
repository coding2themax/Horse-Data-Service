package com.coding2themax.horse.horsedataservice.service;

import java.util.List;
import com.coding2themax.horse.horsedataservice.model.Horse;

//create an interface called HorseService
public interface HorseService {
  /**
   * Represents a horse.
   */
  Horse createHorse(Horse horse);

  Horse getHorseById(Integer id);

  List<Horse> getAllHorses();

  Horse updateHorse(Integer id, Horse horse);

  void deleteHorse(Integer id);
}
