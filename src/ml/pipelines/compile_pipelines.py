"""Compiles the pipelines for every environment into the chart, or checks the chart is current."""

import argparse
import sys
from pathlib import Path

from kfp import compiler

from environments import ENVIRONMENTS
from payment_risk_promote import payment_risk_promote
from payment_risk_rollout import payment_risk_rollout
from payment_risk_training import payment_risk_training

PIPELINES = {
    "payment-risk-training": payment_risk_training,
    "payment-risk-rollout": payment_risk_rollout,
    "payment-risk-promote": payment_risk_promote,
}
CHART_FILES = Path(__file__).resolve().parents[3] / "infra" / "charts" / "ml-training" / "files"


def compile_all(target):
    """Compiles every pipeline for every environment into `target` and returns the YAML by path."""
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    built = {}
    for environment in ENVIRONMENTS:
        (target / environment.name).mkdir(exist_ok=True)
        for name, build in PIPELINES.items():
            file = f"{environment.name}/{name}.yaml"
            compiler.Compiler().compile(build(environment), str(target / file))
            built[file] = (target / file).read_text()
    return built


def main():
    """Compiles into the chart, or with `--check` fails when the chart's files are out of date."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    if arguments.check:
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            built = compile_all(directory)
        stale = [
            file
            for file, text in built.items()
            if not (CHART_FILES / file).exists() or (CHART_FILES / file).read_text() != text
        ]
        if stale:
            sys.exit(f"compiled pipelines are out of date: {', '.join(stale)}")
    else:
        compile_all(CHART_FILES)


if __name__ == "__main__":
    main()
