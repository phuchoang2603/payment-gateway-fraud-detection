import json
import time
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.core import RunIdentifier

from data_platform.contracts import check_physical, prepare
from data_platform.ingest import selected_snapshots, source_frames
from data_platform.products import PORTS
from data_platform.runs import report_key, save_run


def validate(spark, root, manifest, store, run, contracts, image, namespace="bronze"):
    started = time.monotonic()
    report = {
        "run_id": run["run_id"],
        "dataset_version": run["dataset_version"],
        "manifest_sha256": run["manifest_sha256"],
        "source_inventory": run.get("source_inventory"),
        "source_validation": run.get("source_validation"),
        "snapshots": run["snapshots"].copy(),
        "contracts": contracts,
        "image": image,
        "workflow": run["workflow"],
        "run_time": datetime.now(timezone.utc).isoformat(),
        "data_outcome": "error",
        "tables": {},
        "execution_errors": [],
        "sample_limit": 20,
        "driver_sample_limit": 200,
        "input_files": len(manifest["inventory"]),
        "input_bytes": sum(f["bytes"] for f in manifest["inventory"]),
    }
    try:
        frames = selected_snapshots(spark, run, namespace)
        source = source_frames(spark, root, manifest, run)
        for table, frame in frames.items():
            frame.createOrReplaceTempView(table)
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_spark(name="persisted_bronze")
        all_passed = True
        for table in PORTS:
            prepared = prepare(table)
            frame = frames[table]
            check_physical(frame.schema, prepared.schema)
            asset = datasource.add_dataframe_asset(name=table)
            batch = asset.add_batch_definition_whole_dataframe("snapshot_pair")
            suite = context.suites.add(prepared.suite)
            definition = context.validation_definitions.add(
                gx.ValidationDefinition(name=table, data=batch, suite=suite)
            )
            result = definition.run(
                batch_parameters={"dataframe": frame},
                result_format={
                    "result_format": "SUMMARY",
                    "partial_unexpected_count": 20,
                    "include_unexpected_rows": False,
                    "return_unexpected_index_query": False,
                },
                run_id=RunIdentifier(run_name=run["run_id"], run_time=report["run_time"]),
            )
            for check in result.results:
                if (
                    not check.success
                    and not check.exception_info.get("raised_exception")
                    and check.expectation_config.type == "unexpected_rows_expectation"
                ):
                    query = check.expectation_config.kwargs["unexpected_rows_query"].replace("{batch}", table)
                    check.result["details"] = {
                        "unexpected_rows": [json.loads(row) for row in spark.sql(query).limit(20).toJSON().collect()]
                    }
                if check.exception_info.get("raised_exception"):
                    report["execution_errors"].append(
                        {
                            "stage": "validation",
                            "table": table,
                            "rule_id": check.expectation_config.meta.get("id", check.expectation_config.type),
                            "type": "ExpectationExecutionError",
                        }
                    )
            business = prepared.business_columns
            actual = frame.select(*business)
            expected = source[table].select(*business)
            conserved = (
                not actual.exceptAll(expected).limit(1).count() and not expected.exceptAll(actual).limit(1).count()
            )
            rows = frame.count()
            expected_rows = sum(manifest["partitions"][table].values())
            passed = result.success and conserved and rows == expected_rows
            report["tables"][table] = {
                "coverage": prepared.coverage,
                "rows": rows,
                "source_rows": expected_rows,
                "conserved": bool(conserved),
                "success": bool(passed),
                "gx": bounded_result(result.to_json_dict()),
            }
            all_passed = all_passed and passed
        report["data_outcome"] = "error" if report["execution_errors"] else ("passed" if all_passed else "failed")
    except Exception as error:
        report["execution_errors"].append({"type": type(error).__name__, "stage": "validation"})
    finally:
        report["duration_seconds"] = time.monotonic() - started
        store.put(report_key(run, contracts), report)
        run["state"] = "validated" if report["data_outcome"] == "passed" else "validation_" + report["data_outcome"]
        save_run(store, run)
    return report


def bounded_result(value):
    sample_keys = {"unexpected_rows", "partial_unexpected_list", "partial_unexpected_counts", "unexpected_index_list"}
    if isinstance(value, dict):
        return {
            key: (
                [bounded_result(item) for item in item_value[:20]]
                if key in sample_keys and isinstance(item_value, list)
                else bounded_result(item_value)
            )
            for key, item_value in value.items()
        }
    if isinstance(value, list):
        return [bounded_result(item) for item in value]
    return value
