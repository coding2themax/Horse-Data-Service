package com.coding2themax.horse.horsedataservice.service;

import org.springframework.stereotype.Service;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;
import com.coding2themax.horse.horsedataservice.repository.HorseGoodRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class HorseGoodServiceRepoService implements HorseGoodService {

  private HorseGoodRepository horseGoodRepository;

  public HorseGoodServiceRepoService(HorseGoodRepository horseGoodRepository) {
    this.horseGoodRepository = horseGoodRepository;
  }

  @Override
  public Mono<HorseGoods> addHorseGood(HorseGoods horseGood) {
    // The database assigns the id, so a client id can never overwrite a good.
    HorseGoods newGood = new HorseGoods();
    newGood.setName(horseGood.getName());
    newGood.setDescription(horseGood.getDescription());
    return horseGoodRepository.save(newGood);
  }

  @Override
  public Mono<HorseGoods> updateHorseGood(HorseGoods horseGood) {

    return horseGoodRepository.findById(horseGood.getId()).flatMap(h -> {
      h.setName(horseGood.getName());
      h.setDescription(horseGood.getDescription());
      return horseGoodRepository.save(h);
    });
  }

  @Override
  public Mono<HorseGoods> deleteHorseGood(String horseGoodId) {
    return horseGoodRepository.deleteById(Integer.valueOf(horseGoodId)).then(Mono.empty());
  }

  @Override
  public Mono<HorseGoods> getHorseGoodById(String horseGoodId) {

    return horseGoodRepository.findById(Integer.valueOf(horseGoodId));
  }

  @Override
  public Flux<HorseGoods> getAllHorseGoods() {
    return horseGoodRepository.findAll();
  }

}
