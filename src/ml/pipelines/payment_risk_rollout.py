from kfp import dsl

from components import configure, roll_out


@dsl.pipeline(
    name="payment-risk-rollout",
    description="Serve a registered payment-risk version that passed its gate, or return to an earlier one.",
)
def payment_risk_rollout(version: str):
    configure(roll_out(version=version), storage=False)
