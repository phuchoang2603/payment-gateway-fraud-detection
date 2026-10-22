import pytest
import yaml

from data_platform.contracts import contract_path, prepare


@pytest.mark.parametrize("change", ["engine", "scalar", "expectation"])
def test_incomplete_or_incompatible_rule_fails_preparation(tmp_path, change):
    document = yaml.safe_load(contract_path("payment_events").read_text())
    rule = document["schema"][0]["quality"][0]
    if change == "engine":
        rule["engine"] = "duckdb"
    elif change == "scalar":
        rule["implementation"]["kwargs"]["unexpected_rows_query"] = "SELECT count(*) FROM {batch}"
    else:
        rule["implementation"]["type"] = "unimplemented_check"
    path = tmp_path / "contract.yaml"
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(ValueError):
        prepare("payment_events", path)


@pytest.fixture(scope="module")
def business_source(spark, dataset):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data_platform.ingest import source_frames

    root, manifest = dataset
    frames = source_frames(
        spark,
        str(root),
        manifest,
        {
            "dataset_version": "smoke",
            "manifest_sha256": "a" * 64,
            "run_id": "parity",
            "ingested_at": "2026-10-22T00:00:00+00:00",
        },
    )
    source = {}
    for table in frames:
        paths = [root / f["key"] for f in manifest["inventory"] if f["table"] == table]
        source[table] = pa.concat_tables([pq.ParquetFile(p).read() for p in paths], promote_options="permissive")
    return frames, source


def test_spark_business_queries_agree_with_source_rules(spark, business_source):
    import duckdb

    frames, source = business_source
    with duckdb.connect() as connection:
        for table, frame in frames.items():
            connection.register(table, source[table])
            frame.createOrReplaceTempView(table)
        for table in frames:
            contract = yaml.safe_load(contract_path(table, "historical-payments").read_text())
            for source_rule, rule in zip(
                contract["schema"][0]["quality"], prepare(table).document["schema"][0]["quality"]
            ):
                assert source_rule["id"] == rule["id"]
                query = rule["implementation"]["kwargs"]["unexpected_rows_query"].replace("{batch}", table)
                assert connection.execute(source_rule["query"].replace("{object}", table)).fetchone()[0] == 0
                assert spark.sql(query).limit(1).count() == 0, rule["id"]


@pytest.mark.parametrize(
    "case,table,rule_id",
    [
        ("conflict", "payment_events", "payment_delivery_consistency"),
        ("gap", "payment_events", "payment_lifecycle"),
        ("amount", "payment_events", "payment_snapshot_amounts"),
        ("early", "fraud_labels", "label_availability"),
        ("source", "fraud_labels", "label_availability"),
        ("chargeback", "fraud_labels", "label_attempt_linkage"),
    ],
)
def test_invalid_fixtures_agree_across_sql_engines(spark, business_source, case, table, rule_id):
    import duckdb
    import pyarrow as pa

    frames, source = business_source
    records = {name: data.to_pylist() for name, data in source.items()}
    events, labels = records["payment_events"], records["fraud_labels"]
    if case == "conflict":
        from copy import deepcopy

        changed = deepcopy(events[0])
        changed["correlation_id"] = "conflicting-delivery"
        events.append(changed)
    elif case == "gap":
        records["payment_events"] = [e for e in events if e["risk_decision_recorded"] is None]
    elif case == "amount":
        next(e for e in events if e["payment_created"])["payment_created"]["snapshot"]["total"]["minor_units"] = -1
    elif case == "early":
        from datetime import timedelta

        labels[0]["label_available_at"] = labels[0]["occurred_at"] - timedelta(days=1)
    elif case == "source":
        labels[0]["source"] = "INVALID"
    else:
        labels[0]["source"] = "LABEL_SOURCE_CHARGEBACK"
        declined = next(e for e in events if e["attempt_declined"])
        labels[0]["integration_id"] = declined["integration_id"]
        labels[0]["payment_id"] = declined["payment_id"]
        labels[0]["attempt_id"] = declined["attempt_declined"]["attempt_id"]
    connection = duckdb.connect()
    for name in records:
        schema = frames[name].select(*prepare(name).business_columns).schema
        mutated = spark.createDataFrame(records[name], schema=schema)
        mutated.createOrReplaceTempView(name)
        connection.register(name, pa.Table.from_pylist(records[name]))
    source = yaml.safe_load(contract_path(table, "historical-payments").read_text())
    source_rule = next(r for r in source["schema"][0]["quality"] if r["id"] == rule_id)
    bronze_rule = next(r for r in prepare(table).document["schema"][0]["quality"] if r["id"] == rule_id)
    assert connection.execute(source_rule["query"].replace("{object}", table)).fetchone()[0] > 0
    query = bronze_rule["implementation"]["kwargs"]["unexpected_rows_query"].replace("{batch}", table)
    assert spark.sql(query).limit(1).count() > 0


def test_conditional_nested_requirements_agree_with_source(spark, business_source, tmp_path):
    import duckdb
    import pyarrow as pa
    from datacontract.data_contract import DataContract

    from data_platform.contracts import required_predicates

    frames, source = business_source
    frame = frames["payment_events"]
    predicate = required_predicates(prepare("payment_events").document["schema"][0]["properties"])[
        "payment_created.merchant.merchant_id"
    ]
    assert frame.where(predicate).count() == 0
    records = source["payment_events"].to_pylist()
    next(e for e in records if e["payment_created"])["payment_created"]["merchant"]["merchant_id"] = None
    mutated = spark.createDataFrame(records, frame.select(*prepare("payment_events").business_columns).schema)
    assert mutated.where(predicate).count() > 0
    database = tmp_path / "validation.duckdb"
    with duckdb.connect(str(database)) as db:
        db.register("mutated", pa.Table.from_pylist(records, schema=source["payment_events"].schema))
        db.execute("CREATE TABLE payment_events AS SELECT * FROM mutated")
    result = DataContract(
        data_contract_file=str(contract_path("payment_events", "historical-payments")),
        server="validation",
        config={"DATACONTRACT_DUCKDB_DATABASE": str(database), "DATACONTRACT_DUCKDB_SCHEMA": "main"},
    ).test()
    assert any("payment_created.merchant.merchant_id" in c.name and c.result == "failed" for c in result.checks)
