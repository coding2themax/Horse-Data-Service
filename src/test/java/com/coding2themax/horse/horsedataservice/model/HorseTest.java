package com.coding2themax.horse.horsedataservice.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HorseTest {

  @Test
  public void testHorseProperties() {
    Horse horse = new Horse();
    horse.setId(1);
    horse.setName("Max");
    horse.setBreed("Thoroughbred");
    horse.setColor("Chestnut");
    horse.setAge(5);
    horse.setCoatPattern("Blanket");
    horse.setTrainingLevel("Advanced");

    Assertions.assertEquals(1, horse.getId());
    Assertions.assertEquals("Max", horse.getName());
    Assertions.assertEquals("Thoroughbred", horse.getBreed());
    Assertions.assertEquals("Chestnut", horse.getColor());
    Assertions.assertEquals(5, horse.getAge());
    Assertions.assertEquals("Blanket", horse.getCoatPattern());
    Assertions.assertEquals("Advanced", horse.getTrainingLevel());
  }

  @Test
  public void testHorseIsNew() {
    Horse horse = new Horse();
    Assertions.assertTrue(horse.isNew());

    horse.setId(1);
    Assertions.assertFalse(horse.isNew());
  }

  @Test
  public void testHorseSetAsNew() {
    Horse horse = new Horse();
    horse.setAsNew();
    Assertions.assertTrue(horse.isNew());
  }
}
