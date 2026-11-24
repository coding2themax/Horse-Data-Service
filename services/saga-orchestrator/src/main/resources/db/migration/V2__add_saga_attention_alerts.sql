-- NULL on a stuck saga means its alert has not been raised yet.
ALTER TABLE saga_instance ADD COLUMN attention_alerted_at TIMESTAMPTZ;

CREATE INDEX idx_saga_needs_attention ON saga_instance (updated_at)
  WHERE state = 'FAILED_NEEDS_ATTENTION';
