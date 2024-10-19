package com.coding2themax.horse.horsedataservice.model;

import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;

@Table(value = "horse_goods", schema = "stable")
public class HorseGoods implements Persistable<Integer> {
  @Id
  @Column(value = "goods_id")
  private Integer id;

  @Column(value = "goods_name")
  private String name;

  @Column(value = "good_description")
  private String description;

  @Transient
  private boolean newHorseGoods;

  // Add getters and setters
  public Integer getId() {
    return id;
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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean isNew() {
    return this.newHorseGoods || id == null;
  }

}
