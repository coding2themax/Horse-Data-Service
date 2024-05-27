package com.coding2themax.horse.horsedataservice.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table(value = "horse_status", schema = "horse")
public class HorseStatus implements Persistable<Long> {
  @Id
  private Long id;
  @Column("status")
  private String status;

  @Transient
  private boolean newHorseStatus;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public boolean isNew() {
    return this.newHorseStatus || id == null;
  }

  public HorseStatus setAsNew() {
    this.newHorseStatus = true;
    return this;
  }
}
