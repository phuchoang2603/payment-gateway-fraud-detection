"""The `payment-risk-rollout` pipeline: serve a registered version, or return to an earlier one."""

from kfp import dsl

from components import configure, roll_out


def payment_risk_rollout(environment):
    """Returns the rollout pipeline for an environment's node selectors."""

    @dsl.pipeline(
        name="payment-risk-rollout",
        description="Serve a registered payment-risk version that passed its gate, or return to an earlier one.",
    )
    def rollout(version: str, role: str = "champion"):
        configure(
            roll_out(run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER, version=version, role=role),
            nodes=environment.nodes,
            storage=False,
        )

    return rollout
