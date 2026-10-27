import argparse
import fnmatch
import json
import subprocess
from pathlib import Path


def selected_images(current, previous, changed):
    previous = {image["service"]: image for image in previous.get("include", [])}
    result = []
    for image in current["include"]:
        paths = [
            image["dockerfile"],
            str(Path(image["context"]) / ".dockerignore"),
            image["dockerfile"] + ".dockerignore",
            ".github/workflows/build-image.yml",
            *image["inputs"],
        ]
        if previous.get(image["service"]) != image or any(
            file == path
            or file.startswith(path.rstrip("/") + "/")
            or fnmatch.fnmatchcase(file, path)
            for file in changed
            for path in paths
        ):
            result.append(image)
    return {"include": result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("head")
    args = parser.parse_args()
    path = ".github/service-images.json"
    current = json.loads(Path(path).read_text())
    if not args.base or set(args.base) == {"0"}:
        result = current
    else:
        previous = subprocess.run(
            ["git", "show", f"{args.base}:{path}"],
            capture_output=True,
            text=True,
            check=False,
        )
        changed = subprocess.check_output(
            ["git", "diff", "--name-only", "--no-renames", "-z", args.base, args.head],
            text=True,
        ).split("\0")
        result = selected_images(
            current,
            json.loads(previous.stdout) if previous.returncode == 0 else {},
            changed,
        )
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
