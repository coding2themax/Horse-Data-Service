package com.coding2themax.horse.horsedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coding2themax.horse.horsedataservice.model.Horse;
import com.coding2themax.horse.horsedataservice.repository.HorseRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
public class HorseReactiveRepoServiceTest {

  @Mock
  private HorseRepository horseRepository;

  @InjectMocks
  private HorseReactiveRepoService horseReactiveRepoService = new HorseReactiveRepoService(horseRepository);

  @Test
  void testCreateHorse() {
    Horse horse = new Horse();
    horse.setName("Max");
    horse.setId(1);
    horse.setAge(5);
    horse.setBreed("Thoroughbred");

    BDDMockito.when(horseRepository.save(horse)).thenReturn(Mono.just(horse));
    BDDMockito.when(horseRepository.findById(1)).thenReturn(Mono.empty());

    StepVerifier.create(horseReactiveRepoService.createHorse(horse))
        .assertNext(h -> {
          assertNotNull(h.getId());
          assertEquals("Max", h.getName());
          assertEquals(5, h.getAge());
          assertEquals("Thoroughbred", h.getBreed());
        })
        .verifyComplete();
  }

  @Test
  void testGetAllHorses() {
    Horse horse1 = new Horse();
    horse1.setName("Max");
    horse1.setId(1);
    horse1.setAge(5);
    horse1.setBreed("Thoroughbred");

    Horse horse2 = new Horse();
    horse2.setName("Charlie");
    horse2.setId(2);
    horse2.setAge(8);
    horse2.setBreed("Quarter Horse");

    List<Horse> horses = Arrays.asList(horse1, horse2);

    BDDMockito.when(horseRepository.findAll()).thenReturn(Flux.fromIterable(horses));

    StepVerifier.create(horseReactiveRepoService.getAllHorses())
        .expectNext(horse1, horse2)
        .verifyComplete();
  }

  @Test
  void testGetHorseById() {
    Horse horse = new Horse();
    horse.setName("Max");
    horse.setId(1);
    horse.setAge(5);
    horse.setBreed("Thoroughbred");
    BDDMockito.when(horseRepository.findById(1)).thenReturn(Mono.just(horse));
    StepVerifier.create(horseReactiveRepoService.getHorseById("1"))
        .assertNext(h -> {
          assertNotNull(h.getId());
          assertEquals("Max", h.getName());
          assertEquals(5, h.getAge());
          assertEquals("Thoroughbred", h.getBreed());
        })
        .verifyComplete();

  }

  @Test
  void testUpdateHorse() {
    Horse horse = new Horse();
    horse.setName("Max");
    horse.setId(1);
    horse.setAge(5);
    horse.setBreed("Thoroughbred");
    BDDMockito.when(horseRepository.save(horse)).thenReturn(Mono.just(horse));
    BDDMockito.when(horseRepository.findById(1)).thenReturn(Mono.just(horse));

    Horse updatedHorse = new Horse();
    updatedHorse.setName("Maximus");
    updatedHorse.setAge(6);
    updatedHorse.setBreed("Arabian");

    BDDMockito.when(horseRepository.save(updatedHorse)).thenReturn(Mono.just(updatedHorse));
    StepVerifier.create(horseReactiveRepoService.updateHorse("1", updatedHorse))
        .assertNext(h -> {
          assertNotNull(h.getId());
          assertEquals("Maximus", h.getName());
          assertEquals(6, h.getAge());
          assertEquals("Arabian", h.getBreed());
        })
        .verifyComplete();

  }
}
