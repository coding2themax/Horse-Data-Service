package com.coding2themax.horse.horsedataservice.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

import com.coding2themax.horse.horsedataservice.model.Horse;

@Repository
public interface HorseRepository extends ReactiveCrudRepository<Horse, Integer> {

}
