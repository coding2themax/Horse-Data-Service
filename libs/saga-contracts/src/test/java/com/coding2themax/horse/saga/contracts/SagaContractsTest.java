package com.coding2themax.horse.saga.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class SagaContractsTest {

  private static final UUID ORDER = UUID.randomUUID();

  @Test
  void orderLineRejectsInvalidValues() {
    assertThrows(IllegalArgumentException.class, () -> new OrderLine(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new OrderLine(1, 0));
    OrderLine line = new OrderLine(1, 2);
    assertEquals(1, line.productId());
    assertEquals(2, line.quantity());
  }

  @Test
  void orderMessagesRequireLinesAndCopyThem() {
    assertThrows(IllegalArgumentException.class, () -> new SagaMessage.OrderPlaced(ORDER, List.of()));
    assertThrows(IllegalArgumentException.class, () -> new SagaMessage.ReserveStock(ORDER, null));
    List<OrderLine> lines = new ArrayList<>(List.of(new OrderLine(1, 1)));
    SagaMessage.ReserveStock message = new SagaMessage.ReserveStock(ORDER, lines);
    lines.clear();
    assertEquals(1, message.lines().size());
    assertEquals(1, new SagaMessage.OrderPlaced(ORDER, List.of(new OrderLine(1, 1))).lines().size());
  }

  @Test
  void everyMessageExposesItsOrderId() {
    List<SagaMessage> messages = List.of(
        new SagaMessage.StockReserved(ORDER), new SagaMessage.StockRejected(ORDER, "none"),
        new SagaMessage.ReleaseStock(ORDER), new SagaMessage.StockReleased(ORDER),
        new SagaMessage.ApproveOrder(ORDER), new SagaMessage.OrderApproved(ORDER),
        new SagaMessage.OrderApprovalFailed(ORDER, "bad"), new SagaMessage.CancelOrder(ORDER, "bad"),
        new SagaMessage.OrderCancelled(ORDER));
    messages.forEach(message -> assertEquals(ORDER, message.orderId()));
  }

  @Test
  void failureMessagesExposeTheirReason() {
    assertEquals("none", new SagaMessage.StockRejected(ORDER, "none").reason());
    assertEquals("bad", new SagaMessage.OrderApprovalFailed(ORDER, "bad").reason());
    assertEquals("late", new SagaMessage.CancelOrder(ORDER, "late").reason());
  }

  @Test
  void envelopeRequiresEveryField() {
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    assertThrows(NullPointerException.class, () -> new MessageEnvelope(null, id, "t", now, "{}"));
    assertThrows(NullPointerException.class, () -> new MessageEnvelope(id, null, "t", now, "{}"));
    assertThrows(NullPointerException.class, () -> new MessageEnvelope(id, id, null, now, "{}"));
    assertThrows(NullPointerException.class, () -> new MessageEnvelope(id, id, "t", null, "{}"));
    assertThrows(NullPointerException.class, () -> new MessageEnvelope(id, id, "t", now, null));

    MessageEnvelope envelope = new MessageEnvelope(id, ORDER, "t", now, "{}");
    assertEquals(id, envelope.messageId());
    assertEquals(ORDER, envelope.sagaId());
    assertEquals("t", envelope.type());
    assertEquals(now, envelope.occurredAt());
    assertEquals("{}", envelope.payload());
  }

  @Test
  void deadLetterTopicUsesSuffix() {
    assertEquals("horse.stock.commands.dlt", SagaTopics.deadLetter(SagaTopics.STOCK_COMMANDS));
  }
}
