import pytest
import yaml

from data_platform.contracts import contract_path, prepare


def test_native_export_covers_quality_and_conditional_schema():
    events = prepare("payment_events")
    assert "payment_lifecycle" in events.coverage["quality"]
    assert "payment_created.snapshot.items[].unit_price.minor_units" in events.coverage["required"]
    assert "attempt_started.signals.user_agent_family" not in events.coverage["required"]
    assert prepare("fraud_labels").coverage["quality"] == ["label_attempt_linkage", "label_availability"]


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


def test_spark_business_queries_agree_with_source_rules(spark, dataset):
    import duckdb

    from data_platform.ingest import source_frames

    root, manifest = dataset
    run = {
        "dataset_version": "smoke",
        "manifest_sha256": "a" * 64,
        "run_id": "parity",
        "ingested_at": "2026-10-22T00:00:00+00:00",
    }
    frames = source_frames(spark, str(root), manifest, run)
    connection = duckdb.connect()
    for table in frames:
        paths = [str(root / f["key"]) for f in manifest["inventory"] if f["key"].startswith(table + "/")]
        connection.read_parquet(paths, union_by_name=True, hive_partitioning=False).create_view(table)
        frames[table].createOrReplaceTempView(table)
    for table, frame in frames.items():
        source = yaml.safe_load(contract_path(table, "historical-payments").read_text())
        prepared = prepare(table)
        for source_rule, rule in zip(source["schema"][0]["quality"], prepared.document["schema"][0]["quality"]):
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
def test_invalid_fixtures_agree_across_sql_engines(spark, dataset, case, table, rule_id):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from data_platform.ingest import source_frames

    root, manifest = dataset
    run = {
        "dataset_version": "smoke",
        "manifest_sha256": "a" * 64,
        "run_id": "invalid-parity",
        "ingested_at": "2026-10-22T00:00:00+00:00",
    }
    frames = source_frames(spark, str(root), manifest, run)
    records = {}
    for name in frames:
        paths = [root / f["key"] for f in manifest["inventory"] if f["key"].startswith(name + "/")]
        records[name] = pa.concat_tables(
            [pq.ParquetFile(p).read() for p in paths], promote_options="permissive"
        ).to_pylist()
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


def test_conditional_nested_requirements_and_lossless_physical_types(spark, dataset, tmp_path):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from data_platform.contracts import check_physical, required_predicates
    from data_platform.ingest import source_frames

    root, manifest = dataset
    run = {
        "dataset_version": "smoke",
        "manifest_sha256": "a" * 64,
        "run_id": "conditional",
        "ingested_at": "2026-10-22T00:00:00+00:00",
    }
    frame = source_frames(spark, str(root), manifest, run)["payment_events"]
    predicates = required_predicates(prepare("payment_events").document["schema"][0]["properties"])
    predicate = predicates["payment_created.snapshot.total.minor_units"]
    assert frame.where(predicate).count() == 0
    missing = frame.withColumn(
        "payment_created",
        F.col("payment_created").withField(
            "snapshot",
            F.col("payment_created.snapshot").withField(
                "total", F.col("payment_created.snapshot.total").withField("minor_units", F.lit(None).cast("long"))
            ),
        ),
    )
    assert missing.where(predicate).count() > 0
    changed = T.StructType(
        [
            T.StructField(f.name, T.DecimalType(20, 0) if f.name == "aggregate_version" else f.dataType)
            for f in frame.schema
        ]
    )
    with pytest.raises(ValueError, match="schema type differs"):
        check_physical(changed, prepare("payment_events").schema)

    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datacontract.data_contract import DataContract

    paths = [root / f["key"] for f in manifest["inventory"] if f["key"].startswith("payment_events/")]
    source = pa.concat_tables([pq.ParquetFile(p).read() for p in paths], promote_options="permissive")
    records = source.to_pylist()
    next(e for e in records if e["payment_created"])["payment_created"]["merchant"]["merchant_id"] = None
    mutated = spark.createDataFrame(records, frame.select(*prepare("payment_events").business_columns).schema)
    assert mutated.where(predicates["payment_created.merchant.merchant_id"]).count() > 0
    database = tmp_path / "validation.duckdb"
    with duckdb.connect(str(database)) as db:
        db.register("mutated", pa.Table.from_pylist(records, schema=source.schema))
        db.execute("CREATE TABLE payment_events AS SELECT * FROM mutated")
    result = DataContract(
        data_contract_file=str(contract_path("payment_events", "historical-payments")),
        server="validation",
        config={"DATACONTRACT_DUCKDB_DATABASE": str(database), "DATACONTRACT_DUCKDB_SCHEMA": "main"},
    ).test()
    assert not result.has_passed()
    assert any("payment_created.merchant.merchant_id" in c.name and c.result == "failed" for c in result.checks)
