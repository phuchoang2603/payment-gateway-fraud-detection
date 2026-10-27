-- +goose Up
CREATE TABLE assessments (
    assessment_id text PRIMARY KEY,
    integration_id text NOT NULL,
    payment_id text NOT NULL,
    attempt_id text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('NEW', 'ASSESSING', 'COMPLETED', 'ASSESSMENT_ERROR')),
    fence bigint NOT NULL DEFAULT 0,
    first_claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retry_until timestamptz NOT NULL,
    lease_until timestamptz NOT NULL DEFAULT '-infinity',
    retry_after timestamptz NOT NULL DEFAULT '-infinity',
    result bytea,
    completed_at timestamptz,
    UNIQUE (integration_id, payment_id, attempt_id),
    CHECK ((state = 'COMPLETED') = (result IS NOT NULL AND completed_at IS NOT NULL))
);
REVOKE ALL ON TABLE assessments FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO fraud_app;
GRANT SELECT, INSERT ON assessments TO fraud_app;
GRANT UPDATE (state, fence, lease_until, retry_after, result, completed_at) ON assessments TO fraud_app;
-- +goose StatementBegin
DO $$
BEGIN
    EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO fraud_app', current_database());
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION protect_completed_assessment() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'assessment evidence cannot be truncated';
    END IF;
    IF OLD.state = 'COMPLETED' THEN
        RAISE EXCEPTION 'completed assessment is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER completed_assessment_immutable BEFORE UPDATE OR DELETE ON assessments
FOR EACH ROW EXECUTE FUNCTION protect_completed_assessment();
CREATE TRIGGER assessment_evidence_no_truncate BEFORE TRUNCATE ON assessments
FOR EACH STATEMENT EXECUTE FUNCTION protect_completed_assessment();

-- +goose Down
DROP TABLE assessments;
DROP FUNCTION protect_completed_assessment();
