from dataclasses import dataclass, field


@dataclass(frozen=True)
class Environment:
    name: str
    nodes: dict = field(default_factory=dict)
    spark_cpu: str = "3"
    parallelism: int = 4
    plan: tuple = ("4g", "8G")
    stage: tuple = ("22g", "26G")
    assemble: tuple = ("12g", "16G")
    light_memory: str = "12G"
    gate_memory: str = "14G"


PROD = Environment(
    name="prod",
    nodes={"karpenter.sh/nodepool": "aws-cpu-burst", "karpenter.sh/capacity-type": "spot"},
)
DEV = Environment(
    name="dev",
    spark_cpu="1",
    parallelism=1,
    plan=("2g", "4G"),
    stage=("6g", "8G"),
    assemble=("4g", "6G"),
    light_memory="4G",
    gate_memory="6G",
)
ENVIRONMENTS = (PROD, DEV)
