import json
import os
import sys
from pathlib import Path

from simulator.historical import GenerationError, generate, load_generator_config, produce, publish

MODES = ("generate", "shard", "publish")


def main(arguments: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if arguments is None else arguments
    mode = arguments[0] if arguments else "generate"
    if mode not in MODES:
        print(f"unknown historical generation mode: {mode}", file=sys.stderr)
        return 2
    path = os.environ.get("SIMULATOR_CONFIG")
    if not path:
        print("historical generation requires SIMULATOR_CONFIG", file=sys.stderr)
        return 2
    revision = os.environ.get("GIT_REVISION", "")
    try:
        config = load_generator_config(Path(path))
        if mode == "shard":
            index, count = int(os.environ["SHARD_INDEX"]), int(os.environ["SHARD_COUNT"])
            produce(config, revision, (index, count))
            print(json.dumps({"shard": index, "shards": count}))
            return 0
        manifest = generate(config, revision) if mode == "generate" else publish(config, revision)
    except (GenerationError, KeyError, ValueError, OSError) as error:
        print(f"historical generation failed: {error}", file=sys.stderr)
        return 2
    print(
        json.dumps({key: manifest[key] for key in ("dataset_version", "generator_revision", "rows", "physical_bytes")})
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
