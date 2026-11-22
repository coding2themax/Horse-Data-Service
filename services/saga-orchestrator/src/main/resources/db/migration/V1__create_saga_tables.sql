CREATE TABLE saga_instance (
  saga_id UUID PRIMARY KEY,
  order_id UUID NOT NULL UNIQUE,
  state VARCHAR(30) NOT NULL CHECK (state IN (
    'RESERVING_STOCK', 'APPROVING_ORDER', 'RELEASING_STOCK', 'CANCELLING_ORDER',
    'COMPLETED', 'CANCELLED', 'FAILED_NEEDS_ATTENTION')),
  current_command TEXT,
  failure_reason TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  step_deadline TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_active_deadline ON saga_instance (step_deadline)
  WHERE state IN ('RESERVING_STOCK', 'APPROVING_ORDER', 'RELEASING_STOCK', 'CANCELLING_ORDER');

CREATE TABLE saga_step_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  saga_id UUID NOT NULL REFERENCES saga_instance (saga_id),
  from_state VARCHAR(30),
  to_state VARCHAR(30) NOT NULL,
  trigger VARCHAR(50) NOT NULL,
  detail TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_step_log_saga ON saga_step_log (saga_id, id);
