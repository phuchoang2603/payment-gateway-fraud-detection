"""The `payment-risk-promote` pipeline: move traffic to the challenger in analysed steps, then promote or remove it."""

from kfp import dsl

from components import configure, promote_challenger


def payment_risk_promote(environment):
    """Returns the promotion pipeline for an environment's node selectors."""

    @dsl.pipeline(
        name="payment-risk-promote",
        description="Shift traffic to the challenger in steps, check each with an analysis, then promote or remove it.",
    )
    def promote(steps_percent: str = ""):
        configure(
            promote_challenger(run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER, steps_percent=steps_percent),
            nodes=environment.nodes,
            storage=False,
        )

    return promote
