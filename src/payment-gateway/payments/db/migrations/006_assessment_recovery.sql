-- +goose Up
CREATE TABLE assessment_requests (
    attempt_id uuid PRIMARY KEY REFERENCES attempts (attempt_id),
    signals bytea NOT NULL,
    encrypted_token bytea,
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ASSESSING', 'ASSESSMENT_ERROR', 'COMPLETED', 'EXPIRED')),
    fence bigint NOT NULL DEFAULT 0,
    lease_until timestamptz NOT NULL DEFAULT 'epoch',
    retry_after timestamptz NOT NULL DEFAULT 'epoch',
    retry_until timestamptz NOT NULL
);
REVOKE ALL ON assessment_requests FROM PUBLIC;
GRANT SELECT, INSERT ON assessment_requests TO payments_app;
GRANT UPDATE (status, fence, lease_until, retry_after, encrypted_token) ON assessment_requests TO payments_app;

-- +goose Down
DROP TABLE assessment_requests;
