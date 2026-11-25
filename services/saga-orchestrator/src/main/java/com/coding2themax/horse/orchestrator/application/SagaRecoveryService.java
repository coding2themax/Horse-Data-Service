package com.coding2themax.horse.orchestrator.application;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.coding2themax.horse.orchestrator.api.dto.SagaResponse;
import com.coding2themax.horse.orchestrator.infrastructure.SagaStore;

import reactor.core.publisher.Mono;

@Service
public class SagaRecoveryService {

  private final SagaOrchestrator orchestrator;
  private final SagaStore store;
  private final TransactionalOperator transaction;

  public SagaRecoveryService(SagaOrchestrator orchestrator, SagaStore store,
      ReactiveTransactionManager transactionManager) {
    this.orchestrator = orchestrator;
    this.store = store;
    this.transaction = TransactionalOperator.create(transactionManager);
  }

  public Mono<SagaResponse> retry(UUID sagaId) {
    return transaction.transactional(orchestrator.retryFailed(sagaId)).then(Mono.defer(() -> store.find(sagaId)));
  }
}
