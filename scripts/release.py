import json
import os
import re
import subprocess
import sys
from pathlib import Path

RELEASE_FILE = "infra/argocd/app-of-apps/release.yaml"
WORKFLOW = ".github/workflows/release.yml"


def git(*arguments):
    return subprocess.check_output(["git", *arguments], text=True).strip()


def remote_head(branch):
    result = git("ls-remote", "origin", f"refs/heads/{branch}").split()
    return result[0] if result else ""


def release_target():
    revision = os.environ["GITHUB_SHA"]
    environment = os.environ["DEPLOY_ENVIRONMENT"]
    tag = os.environ["DEPLOY_IMAGE_TAG"]
    if environment not in ("dev", "prod") or not re.fullmatch(
        r"[a-f0-9]{40}", revision
    ):
        raise ValueError("invalid release target")
    if not re.fullmatch(rf"(?:pr-[0-9]+|main)-{revision}", tag):
        raise ValueError("image tag does not match the release source")
    return revision, tag, f"deploy/{environment}"


def plan():
    revision, tag, branch = release_target()
    previous = remote_head(branch)
    tags = {}
    if previous:
        git("fetch", "origin", f"refs/heads/{branch}")
        files = git("ls-tree", "--name-only", previous, RELEASE_FILE)
        if files:
            tags = json.loads(git("show", f"{previous}:{RELEASE_FILE}"))["global"].get(
                "imageTags", {}
            )
    images = json.loads(Path(".github/service-images.json").read_text())["include"]
    selected = []
    planned = {}
    common = [
        ".dockerignore",
        ".github/service-images.json",
        WORKFLOW,
        ".github/workflows/build-image.yml",
        "scripts/release.py",
    ]
    for image in images:
        repository = (
            "ghcr.io/" + os.environ["GITHUB_REPOSITORY"].lower() + image["image-suffix"]
        )
        old_tag = tags.get(repository, "")
        match = re.fullmatch(r"(?:pr-[0-9]+|main)-([a-f0-9]{40})", old_tag)
        changed = True
        if match:
            diff = git(
                "diff",
                "--name-only",
                match[1],
                revision,
                "--",
                *common,
                *image["paths"],
            )
            changed = bool(diff)
        planned[repository] = tag if changed else old_tag
        if changed:
            selected.append(image)
    outputs = {
        "matrix": json.dumps({"include": selected}, separators=(",", ":")),
        "image_tags": json.dumps(planned, separators=(",", ":")),
        "previous": previous,
        "build": str(bool(selected)).lower(),
    }
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.writelines(f"{key}={value}\n" for key, value in outputs.items())
    print(
        "Images to build: "
        + (", ".join(image["service"] for image in selected) or "none")
    )


def publish():
    revision, _, branch = release_target()
    source = git("ls-remote", "origin", os.environ["SOURCE_REF"]).split()
    if not source or source[0] != revision:
        print("Source moved; skipping stale release")
        return
    previous = os.environ["PREVIOUS_RELEASE"]
    if remote_head(branch) != previous:
        raise RuntimeError("Deployment changed after planning; rerun this release")
    tags = json.loads(os.environ["IMAGE_TAGS"])
    images = json.loads(Path(".github/service-images.json").read_text())["include"]
    repositories = {
        "ghcr.io/" + os.environ["GITHUB_REPOSITORY"].lower() + image["image-suffix"]
        for image in images
    }
    if set(tags) != repositories or any(
        not re.fullmatch(r"(?:pr-[0-9]+|main)-[a-f0-9]{40}", tag)
        for tag in tags.values()
    ):
        raise ValueError("invalid service image tags")
    release = {"global": {"targetRevision": revision, "imageTags": tags}}
    git("switch", "--force-create", branch, revision)
    Path(RELEASE_FILE).write_text(json.dumps(release, indent=2) + "\n")
    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    git("add", RELEASE_FILE)
    git("commit", "-m", f"Deploy {os.environ['DEPLOY_ENVIRONMENT']} from {revision}")
    git(
        "push",
        f"--force-with-lease=refs/heads/{branch}:{previous}",
        "origin",
        f"HEAD:refs/heads/{branch}",
    )


if __name__ == "__main__":
    {"plan": plan, "publish": publish}[sys.argv[1]]()
