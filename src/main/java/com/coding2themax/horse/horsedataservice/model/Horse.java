package com.coding2themax.horse.horsedataservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.lang.Nullable;

@Table(value = "horse", schema = "horse")
public class Horse implements Persistable<Integer> {

  @Id
  private Integer id;
  private String name;
  private String breed;
  private String color;
  private Integer age;
  private String coatPattern;
  private String trainingLevel;

  @Transient
  private boolean newHorse;

  @Override
  @Nullable
  public Integer getId() {
    return this.id;
  }

  @Override
  public boolean isNew() {
    return this.newHorse || id == null;
  }

  public Horse setAsNew() {
    this.newHorse = true;
    return this;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getBreed() {
    return breed;
  }

  public void setBreed(String breed) {
    this.breed = breed;
  }

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }

  public Integer getAge() {
    return age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public String getCoatPattern() {
    return coatPattern;
  }

  public void setCoatPattern(String coatPattern) {
    this.coatPattern = coatPattern;
  }

  public String getTrainingLevel() {
    return trainingLevel;
  }

  public void setTrainingLevel(String trainingLevel) {
    this.trainingLevel = trainingLevel;
  }

}
