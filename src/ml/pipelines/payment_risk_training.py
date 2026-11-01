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


@dsl.pipeline(
    name="payment-risk-training",
    description="Build the payment_risk_v1 training set, train on one GPU, gate, register and roll out.",
)
def payment_risk_training(
    train: str = "2026-07-29..2026-09-30",
    validation: str = "2026-10-01..2026-10-14",
    test: str = "2026-10-15..2026-10-28",
    cutoff: str = "2026-10-29T00:00:00Z",
    training_set_snapshot: str = "",
    tune_trials: int = 0,
):
    plan = spark_task(
        plan_examples(
            train=train,
            validation=validation,
            test=test,
            cutoff=cutoff,
            existing_snapshot=training_set_snapshot,
        ),
        heap="4g",
        memory="8G",
    )
    with dsl.ParallelFor(items=plan.outputs["chunks"], parallelism=4) as chunk:
        staged = spark_task(stage_examples(chunk=chunk, cutoff=cutoff), heap="22g", memory="26G")
        staged.set_retry(num_retries=2, backoff_duration="60s")
    examples = spark_task(
        assemble_examples(
            train=train,
            validation=validation,
            test=test,
            cutoff=cutoff,
            existing_snapshot=training_set_snapshot,
        ),
        heap="12g",
        memory="16G",
    ).after(staged)
    snapshot = examples.outputs["snapshot"]

    checked = configure(check_examples(snapshot=snapshot), cpu="1", memory="12G")
    split = configure(
        split_windows(snapshot=snapshot, train=train, validation=validation, test=test),
        cpu="1",
        memory="12G",
    ).after(checked)

    tuned = configure(
        tune_params(
            snapshot=snapshot,
            train=train,
            validation=validation,
            test=test,
            trials=tune_trials,
        ),
        storage=False,
    ).after(split)
    params, experiment = tuned.outputs["params"], tuned.outputs["experiment"]

    fitted = configure(
        fit_on_gpu(snapshot=snapshot, train=train, validation=validation, test=test, params=params),
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
        cpu="2",
        memory="14G",
    )
    configure(publish_metrics(summary=verdict.outputs["summary"]), storage=False)
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
        )
        configure(roll_out(version=registered.outputs["version"]), storage=False)
