package com.coding2themax.horse.horsedataservice.repository;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import com.coding2themax.horse.horsedataservice.model.Horse;

import reactor.core.publisher.Flux;

@Repository
public interface HorseRepository extends ReactiveCrudRepository<Horse, Integer> {

  @Query("SELECT * FROM stable.horses WHERE horse_status_id = :status")
  Flux<Horse> findByStatus(Integer status);

}
