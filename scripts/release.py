import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

RELEASE_FILE = "infra/argocd/app-of-apps/release.yaml"
WORKFLOW = ".github/workflows/release.yml"
MANIFEST_TYPES = (
    "application/vnd.oci.image.index.v1+json, "
    "application/vnd.docker.distribution.manifest.list.v2+json, "
    "application/vnd.oci.image.manifest.v1+json, "
    "application/vnd.docker.distribution.manifest.v2+json"
)


def git(*arguments):
    return subprocess.check_output(["git", *arguments], text=True).strip()


def commit_exists(revision):
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


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
        if match and commit_exists(match[1]):
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
    digests = {
        repository: image_digest(repository, tag) for repository, tag in tags.items()
    }
    release = {
        "global": {
            "targetRevision": revision,
            "imageTags": tags,
            "imageDigests": digests,
        }
    }
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


def image_digest(repository, tag):
    """The registry digest of repository:tag, for workloads that only accept digest-pinned images."""
    name = repository.removeprefix("ghcr.io/")
    credentials = base64.b64encode(
        f"x-access-token:{os.environ['GITHUB_TOKEN']}".encode()
    ).decode()
    request = urllib.request.Request(
        f"https://ghcr.io/token?scope=repository:{name}:pull",
        headers={"Authorization": f"Basic {credentials}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(
        f"https://ghcr.io/v2/{name}/manifests/{tag}",
        method="HEAD",
        headers={"Authorization": f"Bearer {token}", "Accept": MANIFEST_TYPES},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        digest = response.headers["Docker-Content-Digest"]
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest or ""):
        raise ValueError(f"no digest for {repository}:{tag}")
    return digest


def workflow_runs(repository, sha):
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/actions/runs?head_sha={sha}&per_page=100",
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["workflow_runs"]


def wait_checks():
    """Wait until every other workflow run for this commit and event has succeeded, so no release skips CI."""
    repository = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["CHECK_SHA"]
    event = os.environ["GITHUB_EVENT_NAME"]
    own = int(os.environ["GITHUB_RUN_ID"])
    deadline = time.monotonic() + int(os.environ.get("CHECK_TIMEOUT_SECONDS", "3600"))
    while True:
        runs = [
            run
            for run in workflow_runs(repository, sha)
            if run["id"] != own and run["event"] == event
        ]
        failed = [
            run
            for run in runs
            if run["status"] == "completed"
            and run["conclusion"] not in ("success", "skipped", "neutral")
        ]
        if failed:
            names = ", ".join(f"{run['name']} ({run['conclusion']})" for run in failed)
            raise SystemExit(f"not releasing {sha}: {names}")
        pending = [run["name"] for run in runs if run["status"] != "completed"]
        if not pending:
            print(
                f"every check on {sha} passed: "
                + ", ".join(sorted(run["name"] for run in runs))
            )
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out waiting for: {', '.join(pending)}")
        time.sleep(20)


if __name__ == "__main__":
    {"plan": plan, "publish": publish, "wait-checks": wait_checks}[sys.argv[1]]()
