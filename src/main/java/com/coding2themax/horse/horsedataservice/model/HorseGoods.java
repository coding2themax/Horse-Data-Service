package com.coding2themax.horse.horsedataservice.model;

import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;

@Table(value = "horse_goods", schema = "stable")
public class HorseGoods {
  @Id
  @Column(value = "id")
  private Long id;

  @Column(value = "name")
  private String name;

  @Column(value = "price")
  private String description;

  // Add getters and setters
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  // Add constructors

  // Add toString method
}
