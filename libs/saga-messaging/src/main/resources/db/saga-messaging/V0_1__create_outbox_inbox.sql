CREATE TABLE outbox (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  message_id UUID NOT NULL UNIQUE,
  saga_id UUID NOT NULL,
  topic VARCHAR(255) NOT NULL,
  envelope TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE inbox (
  consumer VARCHAR(255) NOT NULL,
  message_id UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, message_id)
);
