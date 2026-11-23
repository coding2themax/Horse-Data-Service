package com.coding2themax.horse.saga.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.coding2themax.horse.saga.contracts.MessageEnvelope;
import com.coding2themax.horse.saga.contracts.OrderLine;
import com.coding2themax.horse.saga.contracts.SagaMessage;

class SagaMessageCodecTest {

  private final SagaMessageCodec codec = new SagaMessageCodec();

  @Test
  void roundTripsEnvelopeAndMessage() {
    UUID saga = UUID.randomUUID();
    SagaMessage message = new SagaMessage.ReserveStock(UUID.randomUUID(), List.of(new OrderLine(7, 2)));
    MessageEnvelope envelope = codec.encode(saga, message);
    MessageEnvelope restored = codec.fromJson(codec.toJson(envelope));
    assertEquals(envelope, restored);
    assertEquals("ReserveStock", restored.type());
    assertEquals(message, codec.decode(restored));
  }

  @Test
  void rejectsUnknownTypesAndInvalidJson() {
    UUID id = UUID.randomUUID();
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(new MessageEnvelope(id, id, "Nope", Instant.now(), "{}")));
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(new MessageEnvelope(id, id, "StockReserved", Instant.now(), "not-json")));
    assertThrows(IllegalArgumentException.class, () -> codec.fromJson("not-json"));
  }
}
