import json
import os
import sys
from pathlib import Path

from simulator.historical import GenerationError, generate, load_generator_config, produce, publish

MODES = ("generate", "shard", "publish")
DEFAULT_CONTRACTS = "/app/contracts/historical-payments"


def main() -> int:
    mode = os.environ.get("GENERATOR_MODE", "generate")
    if mode not in MODES:
        print(f"unknown GENERATOR_MODE: {mode}", file=sys.stderr)
        return 2
    path = os.environ.get("SIMULATOR_CONFIG")
    if not path:
        print("historical generation requires SIMULATOR_CONFIG", file=sys.stderr)
        return 2
    contracts = Path(os.environ.get("GENERATOR_CONTRACTS", DEFAULT_CONTRACTS))
    try:
        workers = int(os.environ.get("GENERATOR_WORKERS", "1"))
        config = load_generator_config(Path(path))
        if mode == "shard":
            index, count = int(os.environ["SHARD_INDEX"]), int(os.environ["SHARD_COUNT"])
            produce(config, contracts, (index, count), workers)
            print(json.dumps({"shard": index, "shards": count}))
            return 0
        if mode == "publish":
            manifest = publish(config, contracts)
        else:
            manifest = generate(config, contracts, workers)
    except (GenerationError, KeyError, ValueError, OSError) as error:
        print(f"historical generation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps({key: manifest[key] for key in ("dataset_version", "rows", "physical_bytes")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
