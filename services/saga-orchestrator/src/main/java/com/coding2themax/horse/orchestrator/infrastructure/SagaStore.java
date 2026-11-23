package com.coding2themax.horse.orchestrator.infrastructure;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.stereotype.Repository;

import com.coding2themax.horse.orchestrator.api.dto.SagaResponse;
import com.coding2themax.horse.orchestrator.api.dto.SagaStepResponse;
import com.coding2themax.horse.orchestrator.domain.SagaInstance;
import com.coding2themax.horse.orchestrator.domain.SagaState;

import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class SagaStore {

  private static final String ACTIVE = "('RESERVING_STOCK', 'APPROVING_ORDER', 'RELEASING_STOCK', 'CANCELLING_ORDER')";
  private static final String COLUMNS = "saga_id, order_id, state, current_command, failure_reason, attempts";
  private static final String DEADLINE = "now() + make_interval(secs => :timeoutSeconds)";

  private final DatabaseClient database;

  public SagaStore(DatabaseClient database) {
    this.database = database;
  }

  /** Emits false if this saga already exists. */
  public Mono<Boolean> insert(UUID sagaId, UUID orderId, String command, double timeoutSeconds) {
    return database.sql("INSERT INTO saga_instance (saga_id, order_id, state, current_command, step_deadline) "
        + "VALUES (:sagaId, :orderId, 'RESERVING_STOCK', :command, " + DEADLINE + ") "
        + "ON CONFLICT (saga_id) DO NOTHING")
        .bind("sagaId", sagaId)
        .bind("orderId", orderId)
        .bind("command", command)
        .bind("timeoutSeconds", timeoutSeconds)
        .fetch()
        .rowsUpdated()
        .map(inserted -> inserted == 1);
  }

  public Mono<SagaInstance> lock(UUID sagaId) {
    return database.sql("SELECT " + COLUMNS + " FROM saga_instance WHERE saga_id = :sagaId FOR UPDATE")
        .bind("sagaId", sagaId)
        .map(SagaStore::toInstance)
        .one();
  }

  /** Empty if the saga moved on or another instance is already handling it. */
  public Mono<SagaInstance> lockExpired(UUID sagaId) {
    return database.sql("SELECT " + COLUMNS + " FROM saga_instance WHERE saga_id = :sagaId AND state IN " + ACTIVE
        + " AND step_deadline < now() FOR UPDATE SKIP LOCKED")
        .bind("sagaId", sagaId)
        .map(SagaStore::toInstance)
        .one();
  }

  public Flux<UUID> findExpired(int limit) {
    return database.sql("SELECT saga_id FROM saga_instance WHERE state IN " + ACTIVE
        + " AND step_deadline < now() ORDER BY step_deadline LIMIT :limit")
        .bind("limit", limit)
        .map(row -> row.get("saga_id", UUID.class))
        .all();
  }

  public Mono<Void> applyTransition(UUID sagaId, SagaState next, String command, String reason,
      double timeoutSeconds) {
    GenericExecuteSpec spec = database.sql("UPDATE saga_instance SET state = :state, current_command = :command, "
        + "failure_reason = :reason, attempts = 0, step_deadline = " + DEADLINE + ", updated_at = now() "
        + "WHERE saga_id = :sagaId")
        .bind("state", next.name())
        .bind("timeoutSeconds", timeoutSeconds)
        .bind("sagaId", sagaId);
    return nullable(nullable(spec, "command", command), "reason", reason).fetch().rowsUpdated().then();
  }

  public Mono<Void> recordRetry(UUID sagaId, double timeoutSeconds) {
    return database.sql("UPDATE saga_instance SET attempts = attempts + 1, step_deadline = " + DEADLINE
        + ", updated_at = now() WHERE saga_id = :sagaId")
        .bind("timeoutSeconds", timeoutSeconds)
        .bind("sagaId", sagaId)
        .fetch()
        .rowsUpdated()
        .then();
  }

  public Mono<Void> logStep(UUID sagaId, SagaState from, SagaState to, String trigger, String detail) {
    GenericExecuteSpec spec = database.sql("INSERT INTO saga_step_log (saga_id, from_state, to_state, trigger, detail) "
        + "VALUES (:sagaId, :from, :to, :trigger, :detail)")
        .bind("sagaId", sagaId)
        .bind("to", to.name())
        .bind("trigger", trigger);
    return nullable(nullable(spec, "from", from == null ? null : from.name()), "detail", detail)
        .fetch().rowsUpdated().then();
  }

  public Mono<SagaResponse> find(UUID sagaId) {
    Mono<List<SagaStepResponse>> steps = database.sql("SELECT from_state, to_state, trigger, detail, occurred_at "
        + "FROM saga_step_log WHERE saga_id = :sagaId ORDER BY id")
        .bind("sagaId", sagaId)
        .map(row -> new SagaStepResponse(row.get("from_state", String.class), row.get("to_state", String.class),
            row.get("trigger", String.class), row.get("detail", String.class),
            row.get("occurred_at", OffsetDateTime.class)))
        .all()
        .collectList();
    return database.sql("SELECT saga_id, order_id, state, failure_reason, attempts, created_at, updated_at "
        + "FROM saga_instance WHERE saga_id = :sagaId")
        .bind("sagaId", sagaId)
        .map(row -> new SagaResponse(row.get("saga_id", UUID.class), row.get("order_id", UUID.class),
            SagaState.valueOf(row.get("state", String.class)), row.get("failure_reason", String.class),
            row.get("attempts", Integer.class), row.get("created_at", OffsetDateTime.class),
            row.get("updated_at", OffsetDateTime.class), List.of()))
        .one()
        .zipWith(steps, SagaResponse::withSteps);
  }

  private static SagaInstance toInstance(Readable row) {
    return new SagaInstance(row.get("saga_id", UUID.class), row.get("order_id", UUID.class),
        SagaState.valueOf(row.get("state", String.class)), row.get("current_command", String.class),
        row.get("failure_reason", String.class), row.get("attempts", Integer.class));
  }

  private static GenericExecuteSpec nullable(GenericExecuteSpec spec, String name, String value) {
    return value == null ? spec.bindNull(name, String.class) : spec.bind(name, value);
  }
}
