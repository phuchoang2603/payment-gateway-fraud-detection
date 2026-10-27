import argparse
import json
import re
import subprocess
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode


def published_images(directory):
    images = {}
    files = sorted(directory.glob("*.json"))
    if not files:
        raise ValueError("no published image manifests found")
    for file in files:
        image = json.loads(file.read_text())
        repository = image["repository"]
        if not repository or not re.fullmatch(r"sha256:[a-f0-9]{64}", image["digest"]):
            raise ValueError(f"invalid published image manifest: {file}")
        if not re.fullmatch(r"[a-f0-9]{40}", image["revision"]):
            raise ValueError(f"invalid source revision: {file}")
        if repository in images and images[repository] != image:
            raise ValueError(f"conflicting published images for {repository}")
        images[repository] = image
    return images


def update_values(text, images):
    edits, visited = {}, set()

    def visit(node):
        if node is None or id(node) in visited:
            return
        visited.add(id(node))
        if isinstance(node, MappingNode):
            fields = {key.value: value for key, value in node.value}
            if len(fields) != len(node.value):
                raise ValueError("duplicate YAML mapping keys")
            repository = fields.get("repository")
            digest = fields.get("digest")
            if isinstance(repository, ScalarNode) and isinstance(digest, ScalarNode):
                image = images.get(repository.value)
                if image and digest.value != image["digest"]:
                    for key in ("digest", "revision"):
                        if key in fields:
                            value = fields[key]
                            if not isinstance(value, ScalarNode):
                                raise ValueError(f"image.{key} must be a scalar")
                            edits[value.start_mark.index, value.end_mark.index] = image[
                                key
                            ]
            for _, value in node.value:
                visit(value)
        elif isinstance(node, SequenceNode):
            for value in node.value:
                visit(value)

    visit(yaml.compose(text, Loader=yaml.SafeLoader))
    for (start, end), replacement in sorted(edits.items(), reverse=True):
        text = text[:start] + replacement + text[end:]
    return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifests", type=Path)
    args = parser.parse_args()
    images = published_images(args.manifests)
    files = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "--", "infra/**/values-prod.yaml"]
        )
        .decode()
        .split("\0")
    )
    changes = {}
    for file in filter(None, files):
        path = Path(file)
        original = path.read_text()
        updated = update_values(original, images)
        if updated != original:
            changes[path] = updated
    for path, updated in changes.items():
        path.write_text(updated)
        print(path)


if __name__ == "__main__":
    main()
