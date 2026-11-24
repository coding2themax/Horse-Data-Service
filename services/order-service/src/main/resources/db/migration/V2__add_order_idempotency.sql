-- Orders placed before this migration have no key.
ALTER TABLE orders
  ADD COLUMN idempotency_key UUID UNIQUE,
  ADD COLUMN request_fingerprint TEXT,
  ADD CONSTRAINT orders_idempotency_complete
    CHECK ((idempotency_key IS NULL) = (request_fingerprint IS NULL));
