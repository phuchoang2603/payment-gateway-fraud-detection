"""Where and how big the pipeline steps run in each environment."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Environment:
    """The node selectors and sizes one environment runs the pipeline with."""

    name: str
    nodes: dict = field(default_factory=dict)
    spark_cpu: str = "3"
    parallelism: int = 4
    chunk_days: int = 16
    plan: tuple = ("4g", "8G")
    stage: tuple = ("22g", "26G")
    assemble: tuple = ("12g", "16G")
    light_memory: str = "12G"


PROD = Environment(
    name="prod",
    nodes={"talos.dev/nodepool": "spot"},
    chunk_days=4,
    stage=("6g", "10G"),
    assemble=("6g", "10G"),
)
DEV = Environment(
    name="dev",
    spark_cpu="1",
    parallelism=1,
    plan=("2g", "4G"),
    stage=("6g", "8G"),
    assemble=("4g", "6G"),
    light_memory="4G",
)
ENVIRONMENTS = (PROD, DEV)
