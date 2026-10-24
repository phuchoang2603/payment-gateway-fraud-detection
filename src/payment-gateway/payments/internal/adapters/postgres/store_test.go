package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/wire"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

type fixture struct {
	db    *testsupport.Database
	store *postgres.Store
	mu    sync.Mutex
	now   time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db := testsupport.NewDatabase(t)
	return &fixture{db: db, store: postgres.NewStore(db.App), now: time.Now().UTC().Truncate(time.Microsecond)}
}

func (f *fixture) tick() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(time.Second)
	return f.now
}

func (f *fixture) create(t *testing.T, order string) domain.State {
	t.Helper()
	command := testsupport.NewCreateCommand(testsupport.IntegrationID, order, f.tick())
	state, err := f.store.CreatePayment(context.Background(), application.CreateRequest{
		Command: command,
		Idempotency: application.IdempotencyClaim{
			IntegrationID: testsupport.IntegrationID, Scope: application.ScopeCreatePayment, Key: "key-" + order,
			Digest: []byte(order), PaymentID: uuid.MustParse(command.PaymentID),
		},
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	return state
}

func (f *fixture) transition(state domain.State, command domain.Command) (domain.State, error) {
	return f.store.Transition(context.Background(), application.TransitionRequest{PaymentID: uuid.MustParse(state.PaymentID), Command: command})
}

func (f *fixture) envelope(version uint64) domain.Envelope {
	return domain.Envelope{EventID: uuid.NewString(), Now: f.tick(), ExpectedVersion: version}
}

func (f *fixture) ledgerCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.db.App.QueryRow(context.Background(), `SELECT count(*) FROM payment_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *fixture) startAttempt(t *testing.T, state domain.State) (domain.State, string) {
	t.Helper()
	attemptID := uuid.NewString()
	next, err := f.transition(state, domain.StartAttempt{Envelope: f.envelope(state.Version), AttemptID: attemptID})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	return next, attemptID
}

func (f *fixture) approve(t *testing.T, state domain.State, attemptID string) domain.State {
	t.Helper()
	next, err := f.transition(state, domain.RecordRiskDecision{Envelope: f.envelope(state.Version), AttemptID: attemptID, Assessment: wire.Assessment(testsupport.Approve())})
	if err != nil {
		t.Fatalf("record risk: %v", err)
	}
	return next
}

func TestStaleTransitionIsRejectedWithoutLedgerRow(t *testing.T) {
	f := newFixture(t)
	state := f.create(t, "order-stale")
	before := f.ledgerCount(t)

	_, err := f.transition(state, domain.StartAttempt{Envelope: f.envelope(0), AttemptID: uuid.NewString()})
	if !errors.Is(err, domain.ErrStaleVersion) {
		t.Fatalf("err = %v, want stale version", err)
	}
	if got := f.ledgerCount(t); got != before {
		t.Fatalf("ledger rows = %d, want %d", got, before)
	}
	loaded, err := f.store.LoadPayment(context.Background(), uuid.MustParse(state.PaymentID))
	if err != nil || loaded.Version != 1 || len(loaded.Attempts) != 0 {
		t.Fatalf("payment changed by rejected transition: %+v, %v", loaded, err)
	}
}

func TestConcurrentWritersFromTheSameVersionCommitExactlyOne(t *testing.T) {
	f := newFixture(t)
	state := f.create(t, "order-race")
	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			_, err := f.transition(state, domain.StartAttempt{Envelope: f.envelope(state.Version), AttemptID: uuid.NewString()})
			results <- err
		}()
	}
	committed := 0
	for range cap(results) {
		err := <-results
		switch {
		case err == nil:
			committed++
		case errors.Is(err, domain.ErrStaleVersion):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if committed != 1 {
		t.Fatalf("%d writers committed, want 1", committed)
	}
	assertLedgerMatchesRows(t, f.db.App)
}

func TestLedgerRejectsUpdateDeleteAndTruncateForEveryRole(t *testing.T) {
	f := newFixture(t)
	f.create(t, "order-immutable")
	ctx := context.Background()
	statements := []string{
		`UPDATE payment_events SET event_type = 'tampered'`,
		`DELETE FROM payment_events`,
		`TRUNCATE payment_events`,
	}
	for role, pool := range map[string]*pgxpool.Pool{"owner": f.db.Owner, "runtime": f.db.App} {
		for _, statement := range statements {
			if _, err := pool.Exec(ctx, statement); err == nil {
				t.Errorf("%s role: %q succeeded", role, statement)
			}
		}
	}
	admin, err := pgx.Connect(ctx, f.db.Admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err == nil {
			t.Errorf("superuser: %q succeeded", statement)
		}
	}
	if got := f.ledgerCount(t); got != 1 {
		t.Fatalf("ledger rows = %d, want 1", got)
	}
}

func TestRuntimeRoleHasOnlyInsertAndSelectOnLedger(t *testing.T) {
	f := newFixture(t)
	rows, err := f.db.Owner.Query(context.Background(), `
		SELECT privilege_type FROM information_schema.role_table_grants
		WHERE grantee = 'payments_app' AND table_name = 'payment_events' ORDER BY privilege_type`)
	if err != nil {
		t.Fatal(err)
	}
	privileges, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(privileges) != 2 || privileges[0] != "INSERT" || privileges[1] != "SELECT" {
		t.Fatalf("payments_app privileges on payment_events = %v", privileges)
	}
	var publicConnect, replication bool
	if err := f.db.Owner.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT FROM pg_database d, aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a
			WHERE d.datname = current_database() AND a.grantee = 0 AND a.privilege_type = 'CONNECT'
		), (SELECT rolreplication FROM pg_roles WHERE rolname = 'payments_app')`).Scan(&publicConnect, &replication); err != nil {
		t.Fatal(err)
	}
	if publicConnect || replication {
		t.Fatalf("public database connect = %v, runtime replication = %v", publicConnect, replication)
	}
}

func assertLedgerMatchesRows(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	var mismatches int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM payments p
		WHERE p.version <> (SELECT coalesce(max(aggregate_version), 0) FROM payment_events e WHERE e.payment_id = p.payment_id)
		   OR p.version <> (SELECT count(*) FROM payment_events e WHERE e.payment_id = p.payment_id)`).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf("%d payments whose row version differs from the ledger", mismatches)
	}
	var orphans int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payment_events e WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.payment_id = e.payment_id)`).Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Fatalf("%d ledger events without a payment row", orphans)
	}
}

func TestLatestLedgerVersionEqualsRowVersionForEveryPayment(t *testing.T) {
	f := newFixture(t)
	succeeded := f.create(t, "order-success")
	succeeded, attemptID := f.startAttempt(t, succeeded)
	succeeded = f.approve(t, succeeded, attemptID)
	succeeded, err := f.store.Transition(context.Background(), application.TransitionRequest{
		PaymentID: uuid.MustParse(succeeded.PaymentID),
		Command:   domain.RequestProcessorOperation{Envelope: f.envelope(succeeded.Version), AttemptID: attemptID, OperationKey: "op-" + attemptID},
		ProcessorRequest: &application.ProcessorRequestRecord{
			OperationKey: "op-" + attemptID, PaymentID: uuid.MustParse(succeeded.PaymentID), AttemptID: uuid.MustParse(attemptID),
			IntegrationID: testsupport.IntegrationID, RequestHash: []byte("hash"), EncryptedToken: []byte("sealed"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.transition(succeeded, domain.SucceedPayment{Envelope: f.envelope(succeeded.Version), AttemptID: attemptID, ProcessorReference: "ref"}); err != nil {
		t.Fatal(err)
	}
	expired := f.create(t, "order-expired")
	if _, err := f.transition(expired, domain.ExpirePayment{Envelope: f.envelope(expired.Version)}); err != nil {
		t.Fatal(err)
	}
	f.create(t, "order-open")
	assertLedgerMatchesRows(t, f.db.App)

	rows, err := f.db.App.Query(context.Background(), `SELECT payload FROM payment_events ORDER BY global_position`)
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range payloads {
		event := &paymentsv1.PaymentEvent{}
		if err := proto.Unmarshal(payload, event); err != nil || event.GetEventId() == "" || event.GetPayload() == nil {
			t.Fatalf("ledger payload is not a PaymentEvent: %v", err)
		}
	}
}

func TestFailedTransitionRollsBackStateAttemptAndLedgerTogether(t *testing.T) {
	f := newFixture(t)
	state := f.create(t, "order-rollback")
	state, attemptID := f.startAttempt(t, state)
	state = f.approve(t, state, attemptID)
	ledger := f.ledgerCount(t)

	_, err := f.store.Transition(context.Background(), application.TransitionRequest{
		PaymentID: uuid.MustParse(state.PaymentID),
		Command:   domain.RequestProcessorOperation{Envelope: f.envelope(state.Version), AttemptID: attemptID, OperationKey: "op-" + attemptID},
		ProcessorRequest: &application.ProcessorRequestRecord{
			OperationKey: "a-different-key", PaymentID: uuid.MustParse(state.PaymentID), AttemptID: uuid.MustParse(attemptID),
			IntegrationID: testsupport.IntegrationID, RequestHash: []byte("hash"), EncryptedToken: []byte("sealed"),
		},
	})
	if err == nil {
		t.Fatal("transition with a mismatching processor request succeeded")
	}
	loaded, err := f.store.LoadPayment(context.Background(), uuid.MustParse(state.PaymentID))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != state.Version || loaded.Attempts[0].Phase != domain.PhaseRiskApproved {
		t.Fatalf("rolled back transition left state at version %d phase %s", loaded.Version, loaded.Attempts[0].Phase)
	}
	if got := f.ledgerCount(t); got != ledger {
		t.Fatalf("ledger rows = %d, want %d", got, ledger)
	}
	assertLedgerMatchesRows(t, f.db.App)
}

func TestProcessorRequestPersistsEncryptedWithIntentAndIsClearedByTerminalOutcome(t *testing.T) {
	f := newFixture(t)
	state := f.create(t, "order-crash")
	state, attemptID := f.startAttempt(t, state)
	state = f.approve(t, state, attemptID)
	paymentID, attempt := uuid.MustParse(state.PaymentID), uuid.MustParse(attemptID)
	sealed := []byte("sealed-token-bytes")

	state, err := f.store.Transition(context.Background(), application.TransitionRequest{
		PaymentID: paymentID,
		Command:   domain.RequestProcessorOperation{Envelope: f.envelope(state.Version), AttemptID: attemptID, OperationKey: "op-" + attemptID},
		ProcessorRequest: &application.ProcessorRequestRecord{
			OperationKey: "op-" + attemptID, PaymentID: paymentID, AttemptID: attempt,
			IntegrationID: testsupport.IntegrationID, RequestHash: []byte("hash"), EncryptedToken: sealed,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	restarted := postgres.NewStore(f.db.App)
	record, err := restarted.ProcessorRequest(context.Background(), attempt)
	if err != nil || string(record.EncryptedToken) != string(sealed) || record.OperationKey != "op-"+attemptID {
		t.Fatalf("processor request after restart = %+v, %v", record, err)
	}
	candidates, err := restarted.RecoveryCandidates(context.Background(), f.now.Add(time.Hour), 10)
	if err != nil || len(candidates) != 1 || candidates[0].AttemptID != attempt {
		t.Fatalf("recovery candidates = %+v, %v", candidates, err)
	}

	if _, err := f.transition(state, domain.SucceedPayment{Envelope: f.envelope(state.Version), AttemptID: attemptID, ProcessorReference: "ref"}); err != nil {
		t.Fatal(err)
	}
	record, err = restarted.ProcessorRequest(context.Background(), attempt)
	if err != nil || len(record.EncryptedToken) != 0 || record.OperationKey != "op-"+attemptID || len(record.RequestHash) == 0 {
		t.Fatalf("processor request after terminal outcome = %+v, %v", record, err)
	}
	var plaintextEvents int
	if err := f.db.App.QueryRow(context.Background(), `SELECT count(*) FROM payment_events WHERE position(convert_to('sealed-token', 'UTF8') in payload) > 0`).Scan(&plaintextEvents); err != nil {
		t.Fatal(err)
	}
	if plaintextEvents != 0 {
		t.Fatal("token bytes found in ledger payloads")
	}
}

func TestIdempotencyKeyAndOrderReferenceUniqueness(t *testing.T) {
	f := newFixture(t)
	f.create(t, "order-unique")

	command := testsupport.NewCreateCommand(testsupport.IntegrationID, "order-unique", f.tick())
	_, err := f.store.CreatePayment(context.Background(), application.CreateRequest{
		Command: command,
		Idempotency: application.IdempotencyClaim{
			IntegrationID: testsupport.IntegrationID, Scope: application.ScopeCreatePayment, Key: "key-order-unique",
			Digest: []byte("other"), PaymentID: uuid.MustParse(command.PaymentID),
		},
	})
	if !errors.Is(err, application.ErrIdempotencyExists) {
		t.Fatalf("same key err = %v, want idempotency exists", err)
	}

	command = testsupport.NewCreateCommand(testsupport.IntegrationID, "order-unique", f.tick())
	_, err = f.store.CreatePayment(context.Background(), application.CreateRequest{
		Command: command,
		Idempotency: application.IdempotencyClaim{
			IntegrationID: testsupport.IntegrationID, Scope: application.ScopeCreatePayment, Key: "another-key",
			Digest: []byte("other"), PaymentID: uuid.MustParse(command.PaymentID),
		},
	})
	if !errors.Is(err, application.ErrOrderReferenceTaken) {
		t.Fatalf("same order err = %v, want order reference taken", err)
	}
	var keys int
	if err := f.db.App.QueryRow(context.Background(), `SELECT count(*) FROM idempotency`).Scan(&keys); err != nil || keys != 1 {
		t.Fatalf("idempotency rows = %d, %v; the rejected create must not leave a key", keys, err)
	}
	other := uuid.MustParse("22222222-89ab-4cde-8fab-0123456789ab")
	command = testsupport.NewCreateCommand(other, "order-unique", f.tick())
	if _, err := f.store.CreatePayment(context.Background(), application.CreateRequest{
		Command: command,
		Idempotency: application.IdempotencyClaim{
			IntegrationID: other, Scope: application.ScopeCreatePayment, Key: "key-order-unique",
			Digest: []byte("x"), PaymentID: uuid.MustParse(command.PaymentID),
		},
	}); err != nil {
		t.Fatalf("another integration may reuse the key and order reference: %v", err)
	}
}

func TestSequenceAllocationOrderMayDifferFromCommitOrder(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	slow := f.create(t, "order-slow")
	slow, slowAttempt := f.startAttempt(t, slow)
	slow = f.approve(t, slow, slowAttempt)
	fast := f.create(t, "order-fast")

	blocker, err := f.db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	slowKey := "op-" + slowAttempt
	if _, err := blocker.Exec(ctx, `
		INSERT INTO processor_requests (operation_key, payment_id, attempt_id, integration_id, request_hash, created_at)
		VALUES ($1, $2, $3, $4, 'hash', now())`, slowKey, slow.PaymentID, slowAttempt, testsupport.IntegrationID); err != nil {
		t.Fatal(err)
	}

	slowDone := make(chan error, 1)
	go func() {
		_, err := f.store.Transition(ctx, application.TransitionRequest{
			PaymentID: uuid.MustParse(slow.PaymentID),
			Command:   domain.RequestProcessorOperation{Envelope: f.envelope(slow.Version), AttemptID: slowAttempt, OperationKey: slowKey},
			ProcessorRequest: &application.ProcessorRequestRecord{
				OperationKey: slowKey, PaymentID: uuid.MustParse(slow.PaymentID), AttemptID: uuid.MustParse(slowAttempt),
				IntegrationID: testsupport.IntegrationID, RequestHash: []byte("hash"), EncryptedToken: []byte("sealed"),
			},
		})
		slowDone <- err
	}()
	admin, err := pgx.Connect(ctx, f.db.Admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close(ctx) }()
	testsupport.WaitFor(t, 5*time.Second, func() bool {
		var waiting int
		_ = admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
		select {
		case err := <-slowDone:
			t.Fatalf("slow transition ended early: %v", err)
		default:
		}
		return waiting > 0
	})

	if _, err := f.transition(fast, domain.ExpirePayment{Envelope: f.envelope(fast.Version)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-slowDone:
		t.Fatalf("slow transition finished before the blocker released: %v", err)
	default:
	}
	var committedBefore int
	if err := f.db.App.QueryRow(ctx, `SELECT count(*) FROM payment_events WHERE payment_id = $1 AND aggregate_version = $2`, slow.PaymentID, slow.Version+1).Scan(&committedBefore); err != nil || committedBefore != 0 {
		t.Fatalf("slow event visible before its commit: %d, %v", committedBefore, err)
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("slow transition: %v", err)
	}

	var slowPosition, fastPosition int64
	if err := f.db.App.QueryRow(ctx, `SELECT global_position FROM payment_events WHERE payment_id = $1 AND aggregate_version = $2`, slow.PaymentID, slow.Version+1).Scan(&slowPosition); err != nil {
		t.Fatal(err)
	}
	if err := f.db.App.QueryRow(ctx, `SELECT global_position FROM payment_events WHERE payment_id = $1 AND aggregate_version = $2`, fast.PaymentID, fast.Version+1).Scan(&fastPosition); err != nil {
		t.Fatal(err)
	}
	if slowPosition >= fastPosition {
		t.Fatalf("slow position %d should be lower than fast position %d although it committed later", slowPosition, fastPosition)
	}
	assertLedgerMatchesRows(t, f.db.App)
}

func TestPublicationCoversOnlyLedgerInsertsAndCDCRoleReadsOnlyLedger(t *testing.T) {
	f := newFixture(t)
	provider, _ := telemetry.Traces(t)
	ctx, span := provider.Tracer("test").Start(context.Background(), "create payment")
	defer span.End()
	command := testsupport.NewCreateCommand(testsupport.IntegrationID, "order-cdc", f.tick())
	_, expectedEvent, err := domain.Transition(domain.State{}, command)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := proto.Marshal(wire.Event(expectedEvent))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreatePayment(ctx, application.CreateRequest{
		Command:     command,
		Idempotency: application.IdempotencyClaim{IntegrationID: testsupport.IntegrationID, Scope: application.ScopeCreatePayment, Key: "key-cdc", Digest: []byte("cdc"), PaymentID: uuid.MustParse(command.PaymentID)},
	}); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	var saved *string
	if err := f.db.App.QueryRow(ctx, "SELECT payload, tracingspancontext FROM payment_events").Scan(&payload, &saved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, expected) {
		t.Fatal("trace metadata changed serialized event bytes")
	}
	if saved == nil || *saved != *observe.SerializeContext(ctx) {
		t.Fatal("ledger did not persist transport context")
	}
	if _, err := f.db.App.Exec(ctx, "UPDATE payment_events SET tracingspancontext = NULL"); err == nil {
		t.Fatal("trace metadata bypassed event immutability")
	}

	rows, err := f.db.Owner.Query(ctx, `
		SELECT p.pubname, t.tablename, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate
		FROM pg_publication p JOIN pg_publication_tables t ON t.pubname = p.pubname
		WHERE p.pubname = 'dbz_publication'`)
	if err != nil {
		t.Fatal(err)
	}
	type publication struct {
		Name, Table                   string
		Insert, Update, Delete, Trunc bool
	}
	publications, err := pgx.CollectRows(rows, pgx.RowToStructByPos[publication])
	if err != nil {
		t.Fatal(err)
	}
	if len(publications) != 1 || publications[0].Table != "payment_events" || !publications[0].Insert ||
		publications[0].Update || publications[0].Delete || publications[0].Trunc {
		t.Fatalf("dbz_publication = %+v, want only payment_events with insert", publications)
	}

	cdc, err := pgx.Connect(ctx, f.db.CDCURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cdc.Close(ctx) }()
	var replication bool
	if err := cdc.QueryRow(ctx, `SELECT rolreplication FROM pg_roles WHERE rolname = current_user`).Scan(&replication); err != nil || !replication {
		t.Fatalf("payments_cdc must have REPLICATION: %v, %v", replication, err)
	}
	var count int
	if err := cdc.QueryRow(ctx, `SELECT count(*) FROM payment_events`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("payments_cdc cannot read payment_events: %d, %v", count, err)
	}
	for _, table := range []string{"payments", "attempts", "idempotency", "processor_requests"} {
		if _, err := cdc.Exec(ctx, `SELECT 1 FROM `+table); err == nil {
			t.Errorf("payments_cdc can read %s", table)
		}
	}
	if _, err := cdc.Exec(ctx, `INSERT INTO payment_events (event_id, payment_id, aggregate_version, event_type, payload, occurred_at, recorded_at) VALUES (gen_random_uuid(), gen_random_uuid(), 1, 'x', ''::bytea, now(), now())`); err == nil {
		t.Error("payments_cdc can insert into payment_events")
	}
}

func TestReadinessRequiresAppliedMigrations(t *testing.T) {
	f := newFixture(t)
	if err := postgres.Ready(context.Background(), f.db.App); err != nil {
		t.Fatalf("ready with applied migrations: %v", err)
	}
	if _, err := f.db.Owner.Exec(context.Background(), `UPDATE goose_db_version SET is_applied = false WHERE version_id = (SELECT max(version_id) FROM goose_db_version)`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Ready(context.Background(), f.db.App); err == nil {
		t.Fatal("ready although the latest migration is not applied")
	}
}

func TestLeaderLockIsHeldByOneReplicaAtATime(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := postgres.NewLeader(f.db.App, 42, 20*time.Millisecond)
	second := postgres.NewLeader(f.db.App, 42, 20*time.Millisecond)

	firstLeading := make(chan struct{})
	firstCtx, stopFirst := context.WithCancel(ctx)
	go first.Run(firstCtx, func(workCtx context.Context) {
		close(firstLeading)
		<-workCtx.Done()
	})
	<-firstLeading

	secondLeading := make(chan struct{})
	go second.Run(ctx, func(workCtx context.Context) {
		close(secondLeading)
		<-workCtx.Done()
	})
	select {
	case <-secondLeading:
		t.Fatal("second replica became leader while the first holds the lock")
	case <-time.After(300 * time.Millisecond):
	}
	stopFirst()
	select {
	case <-secondLeading:
	case <-time.After(5 * time.Second):
		t.Fatal("second replica did not take over after the first stopped")
	}
}
