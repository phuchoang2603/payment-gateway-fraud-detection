from kfp import dsl

from components import (
    assemble_examples,
    check_examples,
    configure,
    fit_on_gpu,
    gate,
    plan_examples,
    plan_windows,
    publish_metrics,
    record_version,
    roll_out,
    spark_task,
    split_windows,
    stage_examples,
    tune_params,
)


def payment_risk_training(environment):
    @dsl.pipeline(
        name="payment-risk-training",
        description="Build the payment_risk_v1 training set, train on one GPU, gate, register and roll out.",
    )
    def training(
        train: str = "",
        validation: str = "",
        test: str = "",
        cutoff: str = "",
        training_set_snapshot: str = "",
        tune_trials: int = 0,
        train_days: int = 56,
        validation_days: int = 14,
        test_days: int = 14,
        lag_days: int = 1,
        force: str = "false",
    ):
        windows = configure(
            plan_windows(
                run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                schedule_time=dsl.PIPELINE_JOB_SCHEDULE_TIME_UTC_PLACEHOLDER,
                create_time=dsl.PIPELINE_JOB_CREATE_TIME_UTC_PLACEHOLDER,
                train_days=train_days,
                validation_days=validation_days,
                test_days=test_days,
                lag_days=lag_days,
                force=force,
                fixed_train=train,
                fixed_validation=validation,
                fixed_test=test,
                fixed_cutoff=cutoff,
            ),
            nodes=environment.nodes,
            cpu="1",
            memory=environment.light_memory,
            retries=2,
        )
        train_window = windows.outputs["train"]
        validation_window = windows.outputs["validation"]
        test_window = windows.outputs["test"]
        cutoff_at = windows.outputs["cutoff"]

        with dsl.If(windows.outputs["run"] == "true", name="new-data"):

            def spark(sizes):
                return {
                    "nodes": environment.nodes,
                    "cpu": environment.spark_cpu,
                    "heap": sizes[0],
                    "memory": sizes[1],
                }

            plan = spark_task(
                plan_examples(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                    cutoff=cutoff_at,
                    existing_snapshot=training_set_snapshot,
                ),
                **spark(environment.plan),
            )
            with dsl.ParallelFor(
                items=plan.outputs["chunks"], parallelism=environment.parallelism
            ) as chunk:
                staged = spark_task(
                    stage_examples(chunk=chunk, cutoff=cutoff_at), **spark(environment.stage)
                )
            examples = spark_task(
                assemble_examples(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                    cutoff=cutoff_at,
                    existing_snapshot=training_set_snapshot,
                ),
                **spark(environment.assemble),
            ).after(staged)
            snapshot = examples.outputs["snapshot"]

            checked = configure(
                check_examples(run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER, snapshot=snapshot),
                nodes=environment.nodes,
                cpu="1",
                memory=environment.light_memory,
                retries=2,
            )
            split = configure(
                split_windows(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    snapshot=snapshot,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                ),
                nodes=environment.nodes,
                cpu="1",
                memory=environment.light_memory,
                retries=2,
            ).after(checked)

            tuned = configure(
                tune_params(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    snapshot=snapshot,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                    trials=tune_trials,
                ),
                nodes=environment.nodes,
                storage=False,
            ).after(split)
            params, experiment = tuned.outputs["params"], tuned.outputs["experiment"]

            fitted = configure(
                fit_on_gpu(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    snapshot=snapshot,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                    params=params,
                ),
                nodes=environment.nodes,
                storage=False,
            )
            verdict = configure(
                gate(
                    run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                    snapshot=snapshot,
                    train=train_window,
                    validation=validation_window,
                    test=test_window,
                    model_uri=fitted.outputs["model_uri"],
                ),
                nodes=environment.nodes,
                cpu="2",
                memory=environment.gate_memory,
            )
            configure(
                publish_metrics(summary=verdict.outputs["summary"]),
                nodes=environment.nodes,
                storage=False,
            )
            with dsl.If(verdict.outputs["passed"] == "true", name="passed"):
                registered = configure(
                    record_version(
                        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                        snapshot=snapshot,
                        model_uri=fitted.outputs["model_uri"],
                        report=verdict.outputs["report"],
                        fit=fitted.outputs["fit"],
                        params=params,
                        experiment=experiment,
                    ),
                    nodes=environment.nodes,
                )
                configure(
                    roll_out(
                        run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
                        version=registered.outputs["version"],
                    ),
                    nodes=environment.nodes,
                    storage=False,
                )

    return training
