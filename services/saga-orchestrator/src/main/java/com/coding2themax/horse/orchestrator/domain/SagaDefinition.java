package com.coding2themax.horse.orchestrator.domain;

import java.util.Optional;
import java.util.UUID;

import com.coding2themax.horse.saga.contracts.SagaMessage;
import com.coding2themax.horse.saga.contracts.SagaMessage.ApproveOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.CancelOrder;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApprovalFailed;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderApproved;
import com.coding2themax.horse.saga.contracts.SagaMessage.OrderCancelled;
import com.coding2themax.horse.saga.contracts.SagaMessage.ReleaseStock;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockRejected;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReleased;
import com.coding2themax.horse.saga.contracts.SagaMessage.StockReserved;

/** The store order saga as pure state transitions. */
public final class SagaDefinition {

  private SagaDefinition() {
  }

  /** Returns empty for a late or duplicate reply that does not apply to the current state. */
  public static Optional<Transition> onReply(SagaInstance saga, SagaMessage reply) {
    UUID order = saga.orderId();
    SagaState state = saga.state();
    return switch (reply) {
      case StockReserved r when state == SagaState.RESERVING_STOCK ->
        Optional.of(new Transition(SagaState.APPROVING_ORDER, new ApproveOrder(order, r.lines()), null));
      case StockRejected r when state == SagaState.RESERVING_STOCK ->
        Optional.of(new Transition(SagaState.CANCELLING_ORDER, new CancelOrder(order, r.reason()), r.reason()));
      case OrderApproved r when state == SagaState.APPROVING_ORDER ->
        Optional.of(new Transition(SagaState.COMPLETED, null, null));
      case OrderApprovalFailed r when state == SagaState.APPROVING_ORDER ->
        Optional.of(new Transition(SagaState.RELEASING_STOCK, new ReleaseStock(order), r.reason()));
      case StockReleased r when state == SagaState.RELEASING_STOCK ->
        Optional.of(new Transition(SagaState.CANCELLING_ORDER, new CancelOrder(order, saga.failureReason()),
            saga.failureReason()));
      case OrderCancelled r when state == SagaState.CANCELLING_ORDER ->
        Optional.of(new Transition(SagaState.CANCELLED, null, saga.failureReason()));
      default -> Optional.empty();
    };
  }

  /**
   * Handles a step that has used all its retries. A forward step may have been
   * applied without its reply arriving, so it always releases stock. A stuck
   * compensation cannot be resolved automatically.
   */
  public static Transition onRetriesExhausted(SagaInstance saga) {
    return switch (saga.state()) {
      case RESERVING_STOCK, APPROVING_ORDER -> new Transition(SagaState.RELEASING_STOCK,
          new ReleaseStock(saga.orderId()), "Timed out in " + saga.state());
      case RELEASING_STOCK, CANCELLING_ORDER -> new Transition(SagaState.FAILED_NEEDS_ATTENTION, null,
          "Compensation timed out in " + saga.state() + ": " + saga.failureReason());
      default -> throw new IllegalStateException("Saga is not active: " + saga.state());
    };
  }
}
