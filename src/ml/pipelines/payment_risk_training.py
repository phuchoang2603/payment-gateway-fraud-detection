from kfp import dsl

from components import (
    assemble_examples,
    check_examples,
    configure,
    fit_on_gpu,
    gate,
    plan_examples,
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
        train: str = "2026-07-29..2026-09-30",
        validation: str = "2026-10-01..2026-10-14",
        test: str = "2026-10-15..2026-10-28",
        cutoff: str = "2026-10-29T00:00:00Z",
        training_set_snapshot: str = "",
        tune_trials: int = 0,
    ):
        def spark(sizes):
            return {
                "nodes": environment.nodes,
                "cpu": environment.spark_cpu,
                "heap": sizes[0],
                "memory": sizes[1],
            }

        plan = spark_task(
            plan_examples(
                train=train,
                validation=validation,
                test=test,
                cutoff=cutoff,
                existing_snapshot=training_set_snapshot,
            ),
            **spark(environment.plan),
        )
        with dsl.ParallelFor(
            items=plan.outputs["chunks"], parallelism=environment.parallelism
        ) as chunk:
            staged = spark_task(
                stage_examples(chunk=chunk, cutoff=cutoff), **spark(environment.stage)
            )
        examples = spark_task(
            assemble_examples(
                train=train,
                validation=validation,
                test=test,
                cutoff=cutoff,
                existing_snapshot=training_set_snapshot,
            ),
            **spark(environment.assemble),
        ).after(staged)
        snapshot = examples.outputs["snapshot"]

        checked = configure(
            check_examples(snapshot=snapshot),
            nodes=environment.nodes,
            cpu="1",
            memory=environment.light_memory,
            retries=2,
        )
        split = configure(
            split_windows(snapshot=snapshot, train=train, validation=validation, test=test),
            nodes=environment.nodes,
            cpu="1",
            memory=environment.light_memory,
            retries=2,
        ).after(checked)

        tuned = configure(
            tune_params(
                snapshot=snapshot,
                train=train,
                validation=validation,
                test=test,
                trials=tune_trials,
            ),
            nodes=environment.nodes,
            storage=False,
        ).after(split)
        params, experiment = tuned.outputs["params"], tuned.outputs["experiment"]

        fitted = configure(
            fit_on_gpu(
                snapshot=snapshot, train=train, validation=validation, test=test, params=params
            ),
            nodes=environment.nodes,
            storage=False,
        )
        verdict = configure(
            gate(
                snapshot=snapshot,
                train=train,
                validation=validation,
                test=test,
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
                roll_out(version=registered.outputs["version"]),
                nodes=environment.nodes,
                storage=False,
            )

    return training
