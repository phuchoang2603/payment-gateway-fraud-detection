import argparse
import os
import re
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import yaml
from pipeline_common import quality, readback
from pipeline_common.catalog import expand, native
from pipeline_common.contracts import prepare
from pipeline_common.integrations import workflow
from pipeline_common.products import SILVER_PORTS
from pipeline_common.storage import Store

from silver_pipeline.silver_contracts import contract_path, hashes, product_hash, quality_hash
from silver_pipeline.silver_runs import publication_lock, verify_candidate

QUALITY_ROOT = Path(os.environ.get("DATA_QUALITY_ROOT") or Path(__file__).resolve().parents[4] / "contracts/quality")


@dataclass
class SilverCase:
    table: str
    name: str
    description: str
    sql: str

    @property
    def fqn(self):
        return f"starrocks.lakehouse.silver.{self.table}.{self.name}"


def relation(candidate, table):
    snapshot = candidate["snapshots"][table]
    if type(snapshot) is not int or snapshot <= 0:
        raise ValueError("native quality requires a pinned Silver snapshot")
    return (
        f"(SELECT * FROM lakehouse.silver.{table} FOR VERSION AS OF {snapshot} "
        f"WHERE _candidate_id = '{candidate['candidate_id']}')"
    )


def bronze_count(candidate, table):
    terms = []
    for selected in candidate["inputs"]:
        version = selected["dataset_version"]
        snapshot = selected["snapshots"][table]
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", version)
            or type(snapshot) is not int
            or snapshot <= 0
        ):
            raise ValueError("native quality requires valid pinned Bronze inputs")
        terms.append(
            f"SELECT COUNT(*) AS deliveries FROM lakehouse.bronze.raw_{table} FOR VERSION AS OF {snapshot} "
            f"WHERE _source_dataset_version = '{version}'"
        )
    if not terms:
        raise ValueError("native quality requires accepted Bronze inputs")
    return "(SELECT COALESCE(SUM(deliveries), 0) FROM (" + " UNION ALL ".join(terms) + ") AS source_counts)"


def recurring_lifecycle(events):
    history = "PARTITION BY integration_id, payment_id ORDER BY aggregate_version, event_id"
    return f"""
WITH typed AS (
  SELECT e.*,
    CASE WHEN payment_created IS NOT NULL THEN 'created'
      WHEN attempt_started IS NOT NULL THEN 'started'
      WHEN risk_decision_recorded IS NOT NULL THEN 'decision'
      WHEN processor_operation_requested IS NOT NULL THEN 'requested'
      WHEN processor_outcome_unknown IS NOT NULL THEN 'unknown'
      WHEN attempt_declined IS NOT NULL THEN 'declined'
      WHEN payment_succeeded IS NOT NULL THEN 'succeeded'
      WHEN payment_expired IS NOT NULL THEN 'expired' END AS kind,
    COALESCE(attempt_started.attempt_id, risk_decision_recorded.attempt_id,
      processor_operation_requested.attempt_id, processor_outcome_unknown.attempt_id,
      attempt_declined.attempt_id, payment_succeeded.attempt_id) AS attempt
  FROM {events} AS e
), ordered AS (
  SELECT typed.*,
    LAG(aggregate_version) OVER ({history}) AS previous_version,
    LAG(kind) OVER ({history}) AS previous_kind,
    LAG(attempt) OVER ({history}) AS previous_attempt,
    LAG(occurred_at) OVER ({history}) AS previous_time,
    LAG(correlation_id) OVER ({history}) AS previous_correlation,
    LAG(risk_decision_recorded.assessment.outcome) OVER ({history}) AS previous_outcome
  FROM typed
)
SELECT COUNT(*) FROM ordered AS o
WHERE (kind = 'created' AND aggregate_version <> 1)
  OR (previous_version IS NOT NULL AND aggregate_version <= previous_version)
  OR (previous_version IS NOT NULL AND (occurred_at < previous_time OR correlation_id <> previous_correlation))
  OR previous_kind IN ('succeeded', 'expired')
  OR (aggregate_version = previous_version + 1 AND (
    (kind NOT IN ('created', 'expired', 'started') AND previous_attempt IS NOT NULL AND NOT (attempt <=> previous_attempt))
    OR NOT COALESCE(CASE kind
      WHEN 'created' THEN false
      WHEN 'started' THEN previous_kind IN ('created', 'declined')
      WHEN 'decision' THEN previous_kind = 'started'
      WHEN 'requested' THEN previous_kind = 'decision' AND previous_outcome = 'RISK_OUTCOME_APPROVE'
      WHEN 'unknown' THEN previous_kind = 'requested'
      WHEN 'declined' THEN (previous_kind = 'decision' AND previous_outcome = 'RISK_OUTCOME_DECLINE'
          AND attempt_declined.source = 'DECLINE_SOURCE_RISK')
        OR (previous_kind IN ('requested', 'unknown') AND attempt_declined.source = 'DECLINE_SOURCE_PROCESSOR')
      WHEN 'succeeded' THEN previous_kind IN ('requested', 'unknown')
      WHEN 'expired' THEN previous_kind IN ('created', 'started', 'decision', 'declined')
      ELSE false END, false)))
"""


def cases(candidate):
    if not re.fullmatch(r"[a-f0-9]{64}", candidate.get("candidate_id", "")):
        raise ValueError("native quality requires a valid candidate identity")
    verify_candidate(candidate)
    if set(candidate.get("snapshots", {})) != set(SILVER_PORTS):
        raise ValueError("native quality requires every Silver candidate snapshot")
    if candidate.get("completeness") not in ("complete", "recurring"):
        raise ValueError("native Silver quality requires explicit completeness")
    if (
        candidate.get("contracts") != hashes()
        or candidate.get("product_sha256") != product_hash()
        or candidate.get("quality_sha256") != quality_hash()
    ):
        raise ValueError("native Silver quality requires the candidate contract identities")
    relations = {table: relation(candidate, table) for table in SILVER_PORTS}
    tokens = {"events": relations["payment_events"], "labels": relations["label_observations"]}
    contract = yaml.safe_load((QUALITY_ROOT / "silver-payments.yaml").read_text())
    result = []
    for table in SILVER_PORTS:
        declared = prepare(table, contract_path(table)).rules
        replacements = {"schema": "silver", "table": table, "relation": relations[table]}
        type_queries = [rule.sql.format(**replacements) for rule in declared if rule.id.startswith("schema_type__")]
        required = [
            rule.sql.format(**replacements).split(" AS e WHERE ", 1)[1]
            for rule in declared
            if rule.id.startswith("schema_required__")
        ]
        result.append(
            SilverCase(
                table,
                "schema_types",
                "Persisted physical types match the Silver contract.",
                "SELECT COUNT(*) FROM (SELECT 1) AS anchor WHERE "
                + " OR ".join("(" + query + ") <> 0" for query in type_queries),
            )
        )
        result.append(
            SilverCase(
                table,
                "schema_required",
                "Required fields are present whenever their enclosing value is present.",
                f"SELECT COUNT(*) FROM {relations[table]} AS e WHERE " + " OR ".join(f"({item})" for item in required),
            )
        )
    for source, rules in contract["tables"].items():
        table = "payment_events" if source == "payment_events" else "label_observations"
        for rule in rules:
            if rule.get("type") != "sql" or rule.get("mustBe") != 0:
                raise ValueError("native Silver quality requires zero-violation SQL rules")
            rule_tokens = dict(tokens)
            if candidate["completeness"] == "recurring" and rule["id"] == "label_attempt_linkage":
                rule_tokens["labels"] = (
                    f"(SELECT * FROM {tokens['labels']} AS pending_labels WHERE NOT pending_context)"
                )
            if candidate["completeness"] == "recurring" and rule["id"] == "payment_lifecycle":
                sql = recurring_lifecycle(tokens["events"])
            elif candidate["completeness"] == "recurring" and rule["id"] == "attempt_context_consistency":
                sql = (
                    f"WITH created AS (SELECT DISTINCT integration_id, payment_id, payment_created FROM {tokens['events']} "
                    f"WHERE payment_created IS NOT NULL) SELECT COUNT(*) FROM {tokens['events']} AS e "
                    "JOIN created AS c ON e.integration_id = c.integration_id AND e.payment_id = c.payment_id "
                    "WHERE e.attempt_started IS NOT NULL AND (e.attempt_started.context IS NULL "
                    "OR NOT (TO_JSON(e.attempt_started.context) <=> TO_JSON(c.payment_created)))"
                )
            else:
                sql = rule["query"].format(**rule_tokens)
            result.append(SilverCase(table, rule["id"], rule["description"], sql))
    event_provenance = relations["event_delivery_provenance"]
    label_provenance = relations["label_delivery_provenance"]
    events = relations["payment_events"]
    labels = relations["label_observations"]
    result.extend(
        [
            SilverCase(
                "event_delivery_provenance",
                "delivery_accounting",
                "Every selected event delivery is a representative, duplicate or rejected conflict.",
                f"SELECT COUNT(*) FROM {event_provenance} AS p WHERE p.deliveries <= 0 "
                "OR p.deliveries <> p.representative_deliveries + p.identical_duplicate_deliveries + p.pending_context_deliveries + p.conflicting_deliveries "
                "OR p.conflicting_deliveries <> 0",
            ),
            SilverCase(
                "event_delivery_provenance",
                "source_event_conservation",
                "Event delivery accounting equals the selected Bronze inputs.",
                f"SELECT COUNT(*) FROM (SELECT (SELECT COALESCE(SUM(deliveries), 0) FROM {event_provenance}) AS actual, "
                f"{bronze_count(candidate, 'payment_events')} AS expected) AS counts WHERE actual <> expected",
            ),
            SilverCase(
                "event_delivery_provenance",
                "delivery_event_linkage",
                "Every event delivery maps to a canonical scoped event.",
                f"SELECT COUNT(*) FROM {event_provenance} AS p LEFT JOIN {events} AS e "
                "ON p.integration_id = e.integration_id AND p.event_id = e.event_id WHERE e.event_id IS NULL",
            ),
            SilverCase(
                "payment_events",
                "canonical_delivery_linkage",
                "Every canonical event has exactly one representative delivery.",
                f"SELECT COUNT(*) FROM {events} AS e LEFT JOIN (SELECT integration_id, event_id, "
                f"SUM(representative_deliveries + pending_context_deliveries) AS representatives FROM {event_provenance} "
                "GROUP BY integration_id, event_id) AS p ON e.integration_id = p.integration_id "
                "AND e.event_id = p.event_id WHERE p.representatives IS NULL OR p.representatives <> 1",
            ),
            SilverCase(
                "attempt_histories",
                "complete_attempt_context",
                "Complete histories have one start, one payment creation and one terminal event; incomplete contexts are pending.",
                f"SELECT COUNT(*) FROM {relations['attempt_histories']} AS a "
                + (
                    "WHERE a.pending_context OR a.started_events <> 1 OR a.created_events <> 1 OR a.terminal_events <> 1"
                    if candidate["completeness"] == "complete"
                    else "WHERE a.started_events > 1 OR a.created_events > 1 OR a.terminal_events > 1 "
                    "OR a.pending_context <> (a.started_events = 0 OR a.created_events = 0 OR a.terminal_events = 0 "
                    "OR a.payment_first_version <> 1 OR a.payment_last_version <> a.payment_event_count)"
                ),
            ),
            SilverCase(
                "label_delivery_provenance",
                "label_delivery_linkage",
                "Every label delivery belongs to a retained observation.",
                f"SELECT COUNT(*) FROM {label_provenance} AS p LEFT JOIN {labels} AS l "
                "ON p.observation_id = l.observation_id WHERE p.deliveries <= 0 OR l.observation_id IS NULL",
            ),
            SilverCase(
                "label_delivery_provenance",
                "source_label_conservation",
                "Label delivery accounting equals the selected Bronze inputs.",
                f"SELECT COUNT(*) FROM (SELECT (SELECT COALESCE(SUM(deliveries), 0) FROM {label_provenance}) AS actual, "
                f"{bronze_count(candidate, 'fraud_labels')} AS expected) AS counts WHERE actual <> expected",
            ),
            SilverCase(
                "label_observations",
                "label_identity_and_provenance",
                "Every unique observation has at least one recorded delivery.",
                f"SELECT COUNT(*) FROM {labels} AS l LEFT JOIN (SELECT observation_id, SUM(deliveries) AS deliveries "
                f"FROM {label_provenance} GROUP BY observation_id) AS p ON l.observation_id = p.observation_id "
                "WHERE p.deliveries IS NULL OR p.deliveries < 1",
            ),
            SilverCase(
                "label_observations",
                "label_identity_unique",
                "Observation identifiers are unique.",
                f"SELECT COUNT(*) FROM (SELECT observation_id FROM {labels} GROUP BY observation_id "
                "HAVING COUNT(*) > 1) AS duplicates",
            ),
        ]
    )
    if len({case.fqn for case in result}) != len(result):
        raise ValueError("duplicate native Silver quality rule")
    for case in result:
        case.sql = f"/* dp2 candidate {candidate['candidate_id']} */ " + case.sql
    return result


def expected(candidate, rules):
    return {
        "candidate_id": candidate["candidate_id"],
        "inputs": candidate["inputs"],
        "snapshots": candidate["snapshots"],
        "transformation_revision": candidate["transformation_revision"],
        "contracts": candidate["contracts"],
        "product_sha256": candidate["product_sha256"],
        "quality_sha256": candidate["quality_sha256"],
        "cases": {
            case.fqn: {"table": case.table, "sql_sha256": sha256(case.sql.encode()).hexdigest()} for case in rules
        },
    }


def observed(table):
    found, after = {}, None
    while True:
        query = {
            "entityLink": f"<#E::table::starrocks.lakehouse.silver.{table}>",
            "fields": "testCaseResult",
            "limit": readback.PAGE,
        }
        if after:
            query["after"] = after
        page = readback.get("dataQuality/testCases", query)
        found.update({case["fullyQualifiedName"]: case for case in page.get("data", [])})
        next_page = page.get("paging", {}).get("after")
        if not next_page:
            return found
        if next_page == after:
            raise ValueError("native Silver test-case pagination did not advance")
        after = next_page


def cancel_owned_queries(cursor, username, own_connection, candidate_id):
    marker = f"/* dp2 candidate {candidate_id} */"
    cursor.execute("SHOW FULL PROCESSLIST")
    for query in cursor.fetchall():
        if (
            query["User"] == username
            and int(query["Id"]) != own_connection
            and query["Command"] == "Query"
            and marker in (query.get("Info") or "")
        ):
            cursor.execute(f"KILL QUERY {int(query['Id'])}")


def quiesce(candidate_id):
    import pymysql

    config = expand(quality.connection())
    host, port = config["hostPort"].rsplit(":", 1)
    with pymysql.connect(
        host=host,
        port=int(port),
        user=config["username"],
        password=config["password"],
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=30,
    ) as connection:
        with connection.cursor() as cursor:
            cancel_owned_queries(cursor, config["username"], connection.thread_id(), candidate_id)


def run(stage, store, candidate):
    rules = cases(candidate)
    prefix = f"dp2/candidates/{candidate['candidate_id']}"
    if stage == "discover":
        prepared = expected(candidate, rules)
        previous = store.get(f"{prefix}/native-expected.json")
        if previous is None:
            previous = {**prepared, "prepared_at": int(time.time() * 1000)}
            store.put(f"{prefix}/native-expected.json", previous, create=True)
        elif any(previous.get(key) != value for key, value in prepared.items()):
            raise ValueError("native Silver rule set or pinned inputs changed")
        config = workflow("starrocks")
        source = config["source"]["sourceConfig"]["config"]
        source["schemaFilterPattern"] = {"includes": ["^silver$"]}
        source["tableFilterPattern"] = {"includes": ["^(" + "|".join(SILVER_PORTS) + ")$"]}
        receipt = {
            "candidate_id": candidate["candidate_id"],
            "snapshots": candidate["snapshots"],
            "started_at": int(time.time() * 1000),
            "attempt_id": str(uuid4()),
            "outcome": "error",
        }
        try:
            native("ingest", config)
            receipt["outcome"] = "passed"
        except Exception as error:
            receipt["error_type"] = type(error).__name__
            raise
        finally:
            receipt["ended_at"] = int(time.time() * 1000)
            store.put(f"{prefix}/native-discovery/{receipt['attempt_id']}.json", receipt, create=True)
            store.put(f"{prefix}/native-discovery.json", receipt)
        return
    if stage != "validate":
        raise ValueError("unsupported native Silver stage")
    prepared = store.get(f"{prefix}/native-expected.json")
    discovery = store.get(f"{prefix}/native-discovery.json")
    if not prepared or any(prepared.get(key) != value for key, value in expected(candidate, rules).items()):
        raise ValueError("native Silver rule set was not prepared for this candidate")
    if not discovery or discovery.get("outcome") != "passed" or discovery.get("snapshots") != candidate["snapshots"]:
        raise ValueError("native Silver discovery is missing or stale")
    result = {
        "candidate_id": candidate["candidate_id"],
        "snapshots": candidate["snapshots"],
        "started_at": int(time.time() * 1000),
        "attempt_id": str(uuid4()),
        "outcome": "error",
    }
    try:
        for table in SILVER_PORTS:
            config = quality.test_workflow(table, rules, f"starrocks.lakehouse.silver.{table}")
            config["source"]["sourceConfig"]["config"]["entityFullyQualifiedName"] = (
                f"starrocks.lakehouse.silver.{table}"
            )
            native("test", config)
        found = {key: value for table in SILVER_PORTS for key, value in observed(table).items()}
        problems = readback.verify(prepared, found, result["started_at"])
        result.update(problems=dict(sorted(problems.items())[:50]), problem_count=len(problems))
        result["outcome"] = "failed" if problems else "passed"
        if problems:
            raise ValueError("native Silver quality results are incomplete or failed")
    except Exception as error:
        result["error_type"] = type(error).__name__
        try:
            quiesce(candidate["candidate_id"])
        except Exception as cleanup_error:
            result["cleanup_error_type"] = type(cleanup_error).__name__
        raise
    finally:
        result["ended_at"] = int(time.time() * 1000)
        store.put(f"{prefix}/native-validation/{result['attempt_id']}.json", result, create=True)
        store.put(f"{prefix}/native-validation.json", result)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dp2-native")
    parser.add_argument("stage", choices=("discover", "validate", "publish"))
    parser.add_argument(
        "--evidence",
        default=os.environ.get("SILVER_EVIDENCE_ROOT"),
        required=not os.environ.get("SILVER_EVIDENCE_ROOT"),
    )
    parser.add_argument(
        "--candidate-id",
        default=os.environ.get("SILVER_CANDIDATE_ID"),
        required=not os.environ.get("SILVER_CANDIDATE_ID"),
    )
    parser.add_argument("--takeover-stopped-publisher", action="store_true")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-f0-9]{64}", args.candidate_id):
        parser.error("invalid candidate identity")
    store = Store(args.evidence)
    candidate = store.get(f"dp2/candidates/{args.candidate_id}.json")
    allowed = ("candidate", "accepted") if args.stage == "publish" else ("candidate",)
    if not candidate or candidate.get("state") not in allowed:
        raise ValueError("native Silver validation requires an unaccepted candidate")
    if args.stage == "publish":
        from silver_pipeline.silver_governance import publish

        publish(store, candidate, takeover_stopped=args.takeover_stopped_publisher)
        return 0
    with publication_lock(store, args.candidate_id, os.environ["WORKFLOW_ID"], args.takeover_stopped_publisher):
        run(args.stage, store, candidate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
