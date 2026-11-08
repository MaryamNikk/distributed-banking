CREATE TABLE accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    CHECK (account_id ~ '^A[0-9]{4}$'),
    CHECK (account_id BETWEEN 'A0001' AND 'A1000')
);

INSERT INTO accounts (account_id, balance) VALUES
    ('A0100', 1000),
    ('A0200', 2000),
    ('A0500', 1000);
CREATE TABLE IF NOT EXISTS participant_results (
    tx_id TEXT PRIMARY KEY,
    account_id VARCHAR(5) NOT NULL,
    delta BIGINT NOT NULL CHECK (delta <> 0),
    decision TEXT NOT NULL CHECK (decision IN ('COMMIT', 'ABORT'))
);
