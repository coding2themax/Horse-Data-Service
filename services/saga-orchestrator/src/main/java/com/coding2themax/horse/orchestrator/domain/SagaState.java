package com.coding2themax.horse.orchestrator.domain;

/** Stored by name in saga_instance.state. */
public enum SagaState {
  RESERVING_STOCK,
  APPROVING_ORDER,
  RELEASING_STOCK,
  CANCELLING_ORDER,
  COMPLETED,
  CANCELLED,
  FAILED_NEEDS_ATTENTION
}
