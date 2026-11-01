import argparse
import sys
from pathlib import Path

from kfp import compiler

from payment_risk_rollout import payment_risk_rollout
from payment_risk_training import payment_risk_training

PIPELINES = {
    "payment-risk-training": payment_risk_training,
    "payment-risk-rollout": payment_risk_rollout,
}
CHART_FILES = Path(__file__).resolve().parents[3] / "infra" / "charts" / "ml-training" / "files"


def compile_all(target):
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    for name, pipeline in PIPELINES.items():
        compiler.Compiler().compile(pipeline, str(target / f"{name}.yaml"))
    return {name: (target / f"{name}.yaml").read_text() for name in PIPELINES}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    if arguments.check:
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            built = compile_all(directory)
        stale = [
            name
            for name, text in built.items()
            if not (CHART_FILES / f"{name}.yaml").exists()
            or (CHART_FILES / f"{name}.yaml").read_text() != text
        ]
        if stale:
            sys.exit(f"compiled pipelines are out of date: {', '.join(stale)}")
    else:
        compile_all(CHART_FILES)


if __name__ == "__main__":
    main()
