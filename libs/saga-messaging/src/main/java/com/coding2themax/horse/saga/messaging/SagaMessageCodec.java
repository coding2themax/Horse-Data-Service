package com.coding2themax.horse.saga.messaging;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** Converts saga messages to and from a wire format independent of app Jackson settings. */
public class SagaMessageCodec {

  private static final Map<String, Class<?>> TYPES = Arrays.stream(SagaMessage.class.getPermittedSubclasses())
      .collect(Collectors.toUnmodifiableMap(Class::getSimpleName, type -> type));

  private final ObjectMapper mapper = JsonMapper.builder()
      .addModule(new JavaTimeModule())
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .build();

  public MessageEnvelope encode(UUID sagaId, SagaMessage message) {
    return new MessageEnvelope(UUID.randomUUID(), sagaId, message.getClass().getSimpleName(), Instant.now(),
        write(message));
  }

  public SagaMessage decode(MessageEnvelope envelope) {
    Class<?> type = TYPES.get(envelope.type());
    if (type == null) {
      throw new IllegalArgumentException("Unknown saga message type: " + envelope.type());
    }
    try {
      return (SagaMessage) mapper.readValue(envelope.payload(), type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid payload for " + envelope.type(), e);
    }
  }

  public String toJson(MessageEnvelope envelope) {
    return write(envelope);
  }

  public MessageEnvelope fromJson(String json) {
    try {
      return mapper.readValue(json, MessageEnvelope.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid saga message envelope", e);
    }
  }

  private String write(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Could not serialize saga message", e);
    }
  }
}
