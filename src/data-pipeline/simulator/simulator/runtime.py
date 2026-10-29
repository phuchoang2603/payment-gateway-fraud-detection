import json
import os
import sys
from pathlib import Path

from simulator.historical import GenerationError, generate, load_generator_config


def main() -> int:
    path = os.environ.get("SIMULATOR_CONFIG")
    if not path:
        print("historical generation requires SIMULATOR_CONFIG", file=sys.stderr)
        return 2
    try:
        manifest = generate(load_generator_config(Path(path)), os.environ.get("GIT_REVISION", ""))
    except (GenerationError, ValueError, OSError) as error:
        print(f"historical generation failed: {error}", file=sys.stderr)
        return 2
    print(
        json.dumps({key: manifest[key] for key in ("dataset_version", "generator_revision", "rows", "physical_bytes")})
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
