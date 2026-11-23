package com.coding2themax.horse.saga.contracts;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Metadata and JSON payload sent for each saga message. */
public record MessageEnvelope(UUID messageId, UUID sagaId, String type, Instant occurredAt, String payload) {

  public MessageEnvelope {
    Objects.requireNonNull(messageId, "messageId");
    Objects.requireNonNull(sagaId, "sagaId");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(occurredAt, "occurredAt");
    Objects.requireNonNull(payload, "payload");
  }
}
