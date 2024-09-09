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
    return horseGoodRepository.save(horseGood);
  }

  @Override
  public Mono<HorseGoods> updateHorseGood(HorseGoods horseGood) {
    return horseGoodRepository.save(horseGood);
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
