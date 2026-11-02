from kfp import dsl

from components import configure, roll_out


def payment_risk_rollout(environment):
    @dsl.pipeline(
        name="payment-risk-rollout",
        description="Serve a registered payment-risk version that passed its gate, or return to an earlier one.",
    )
    def rollout(version: str):
        configure(
            roll_out(run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER, version=version),
            nodes=environment.nodes,
            storage=False,
        )

    return rollout
