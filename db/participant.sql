CREATE TABLE IF NOT EXISTS participant_results (
    tx_id TEXT PRIMARY KEY,
    account_id VARCHAR(5) NOT NULL,
    delta BIGINT NOT NULL CHECK (delta <> 0),
    decision TEXT NOT NULL CHECK (decision IN ('COMMIT', 'ABORT'))
);
