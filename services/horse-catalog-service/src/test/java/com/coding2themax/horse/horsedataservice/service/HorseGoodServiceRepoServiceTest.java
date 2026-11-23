package com.coding2themax.horse.horsedataservice.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coding2themax.horse.horsedataservice.model.HorseGoods;
import com.coding2themax.horse.horsedataservice.repository.HorseGoodRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
public class HorseGoodServiceRepoServiceTest {

  @Mock
  private HorseGoodRepository horseGoodRepository;

  @InjectMocks
  private HorseGoodServiceRepoService horseGoodServiceRepoService = new HorseGoodServiceRepoService(
      horseGoodRepository);

  @Test
  void testAddHorseGood() {
    // Generate a horseGood object
    HorseGoods horseGood = new HorseGoods();
    horseGood.setId(1);
    horseGood.setName("Saddle");
    horseGood.setDescription("description");

    // Mock the save method of the repository
    BDDMockito.when(horseGoodRepository.save(BDDMockito.any(HorseGoods.class))).thenReturn(Mono.just(horseGood));

    // Call the addHorseGood method of the service
    Mono<HorseGoods> result = horseGoodServiceRepoService.addHorseGood(horseGood);

    // Verify the result
    ArgumentCaptor<HorseGoods> saved = ArgumentCaptor.forClass(HorseGoods.class);
    BDDMockito.then(horseGoodRepository).should().save(saved.capture());
    Assertions.assertNull(saved.getValue().getId());
    Assertions.assertTrue(saved.getValue().isNew());
    Assertions.assertEquals("Saddle", saved.getValue().getName());
    Assertions.assertEquals("description", saved.getValue().getDescription());
    StepVerifier.create(result).expectNext(horseGood).verifyComplete();

  }

  @Test
  void testDeleteHorseGood() {
    // Generate a horseGoodId
    String horseGoodId = "1";

    // Mock the deleteById method of the repository
    BDDMockito.when(horseGoodRepository.deleteById(Integer.valueOf(horseGoodId))).thenReturn(Mono.empty());

    // Call the deleteHorseGood method of the service
    Mono<HorseGoods> result = horseGoodServiceRepoService.deleteHorseGood(horseGoodId);

    // Verify the result
    BDDMockito.then(horseGoodRepository).should().deleteById(Integer.valueOf(horseGoodId));
    StepVerifier.create(result).verifyComplete();
  }

  @Test
  void testGetAllHorseGoods() {
    // Mock the findAll method of the repository
    BDDMockito.when(horseGoodRepository.findAll()).thenReturn(Flux.empty());

    // Call the getAllHorseGoods method of the service
    Flux<HorseGoods> result = horseGoodServiceRepoService.getAllHorseGoods();

    // Verify the result
    BDDMockito.then(horseGoodRepository).should().findAll();
    StepVerifier.create(result).verifyComplete();

  }

  @Test
  void testGetHorseGoodById() {
    // Generate a horseGoodId
    String horseGoodId = "1";

    // Generate a horseGood object
    HorseGoods horseGood = new HorseGoods();
    horseGood.setId(1);
    horseGood.setName("Saddle");

    // Mock the findById method of the repository
    BDDMockito.when(horseGoodRepository.findById(Integer.valueOf(horseGoodId))).thenReturn(Mono.just(horseGood));

    // Call the getHorseGoodById method of the service
    Mono<HorseGoods> result = horseGoodServiceRepoService.getHorseGoodById(horseGoodId);

    // Verify the result
    BDDMockito.then(horseGoodRepository).should().findById(Integer.valueOf(horseGoodId));
    StepVerifier.create(result).expectNext(horseGood).verifyComplete();
  }

  @Test
  void testUpdateHorseGood() {
    // Generate a horseGood object
    HorseGoods horseGood = new HorseGoods();
    horseGood.setId(1);
    horseGood.setName("Saddle");

    HorseGoods updatedHorseGood = new HorseGoods();
    updatedHorseGood.setId(1);
    updatedHorseGood.setName("Western Saddle");

    BDDMockito.when(horseGoodRepository.findById(1)).thenReturn(Mono.just(horseGood));
    // Mock the save method of the repository
    BDDMockito.when(horseGoodRepository.save(BDDMockito.any(HorseGoods.class))).thenReturn(Mono.just(updatedHorseGood));

    // Call the updateHorseGood method of the service
    Mono<HorseGoods> result = horseGoodServiceRepoService.updateHorseGood(updatedHorseGood);

    // Verify the result
    // BDDMockito.then(horseGoodRepository).should().save(horseGood);
    StepVerifier.create(result).expectNext(updatedHorseGood).verifyComplete();
  }

}
