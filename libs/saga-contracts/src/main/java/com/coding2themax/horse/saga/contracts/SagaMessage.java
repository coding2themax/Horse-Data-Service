package com.coding2themax.horse.saga.contracts;

import java.util.List;
import java.util.UUID;

/**
 * Every event, command, and reply in the store order saga. Record names are
 * the wire-format message types, so renaming one is a breaking change.
 */
public sealed interface SagaMessage {

  UUID orderId();

  record OrderPlaced(UUID orderId, List<OrderLine> lines) implements SagaMessage {
    public OrderPlaced {
      lines = OrderLine.requireLines(lines);
    }
  }

  record ReserveStock(UUID orderId, List<OrderLine> lines) implements SagaMessage {
    public ReserveStock {
      lines = OrderLine.requireLines(lines);
    }
  }

  record StockReserved(UUID orderId) implements SagaMessage {
  }

  record StockRejected(UUID orderId, String reason) implements SagaMessage {
  }

  record ReleaseStock(UUID orderId) implements SagaMessage {
  }

  record StockReleased(UUID orderId) implements SagaMessage {
  }

  record ApproveOrder(UUID orderId) implements SagaMessage {
  }

  record OrderApproved(UUID orderId) implements SagaMessage {
  }

  record OrderApprovalFailed(UUID orderId, String reason) implements SagaMessage {
  }

  record CancelOrder(UUID orderId, String reason) implements SagaMessage {
  }

  record OrderCancelled(UUID orderId) implements SagaMessage {
  }
}
