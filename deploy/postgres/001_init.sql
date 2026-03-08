CREATE TABLE IF NOT EXISTS probe_batches (
    batch_id UUID PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL,
    sentinel_id TEXT NOT NULL,
    sentinel_location TEXT NOT NULL,
    auth JSONB NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS endpoint_samples (
    batch_id UUID NOT NULL REFERENCES probe_batches(batch_id) ON DELETE CASCADE,
    sentinel_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL,
    provider TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (batch_id, sentinel_id, endpoint_id)
);

CREATE INDEX IF NOT EXISTS endpoint_samples_sampled_at_idx
    ON endpoint_samples (sampled_at DESC);
CREATE INDEX IF NOT EXISTS endpoint_samples_provider_idx
    ON endpoint_samples (provider);
