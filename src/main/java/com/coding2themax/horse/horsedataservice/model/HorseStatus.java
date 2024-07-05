package com.coding2themax.horse.horsedataservice.model;

import java.util.List;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import reactor.core.publisher.Mono;

@Table(value = "horse_status", schema = "horse")
public class HorseStatus implements Persistable<Long> {
  @Id
  private Long id;
  @Column("status")
  private String status;

  private List<Horse> horses;

  @Transient
  private boolean newHorseStatus;

  public Long getId() {
    return id;
  }

  public List<Horse> getHorses() {
    return horses;
  }

  public void setHorses(List<Horse> horses) {
    this.horses = horses;
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

  public static Builder builder() {
    return new Builder();
  }

  public static Mono<HorseStatus> fromRows(List<Map<String, Object>> rows) {
    return Mono.justOrEmpty(rows.stream().findFirst().map(row -> {
      HorseStatus horseStatus = new HorseStatus();
      horseStatus.setId((Long) row.get("id"));
      horseStatus.setStatus((String) row.get("status"));
      horseStatus.setHorses(rows.stream().map(Horse::fromRow).toList();
      return horseStatus;
    }));
  }

  public static class Builder {
    private Long id;
    private String status;
    private Horse horse;
    private boolean newHorseStatus;

    private Builder() {
    }

    public Builder id(Long id) {
      this.id = id;
      return this;
    }

    public Builder status(String status) {
      this.status = status;
      return this;
    }

    public Builder horse(Horse horse) {
      this.horse = horse;
      return this;
    }

    public Builder newHorseStatus(boolean newHorseStatus) {
      this.newHorseStatus = newHorseStatus;
      return this;
    }

    public HorseStatus build() {
      HorseStatus horseStatus = new HorseStatus();
      horseStatus.setId(id);
      horseStatus.setStatus(status);
      horseStatus.setHorse(horse);
      horseStatus.newHorseStatus = newHorseStatus;
      return horseStatus;
    }
  }

}
