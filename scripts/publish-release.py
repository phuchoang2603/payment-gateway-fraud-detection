import json
import os
import re
import subprocess
from pathlib import Path

import yaml


def git(*arguments):
    return subprocess.check_output(["git", *arguments], text=True).strip()


def image_values(value, images):
    if not isinstance(value, dict):
        return {}
    repository = value.get("repository")
    if repository in images:
        return images[repository]
    return {
        key: override
        for key, child in value.items()
        if (override := image_values(child, images))
    }


def main():
    revision = os.environ["GITHUB_SHA"]
    environment = os.environ["DEPLOY_ENVIRONMENT"]
    if environment not in ("dev", "prod") or not re.fullmatch(
        r"[a-f0-9]{40}", revision
    ):
        raise ValueError("invalid release target")
    source = os.environ["SOURCE_REF"]
    current = git("ls-remote", "origin", source).split()
    if not current or current[0] != revision:
        print("Source moved; skipping stale release")
        return
    manifests = Path(".image-manifests")
    services = json.loads(Path(".github/service-images.json").read_text())["include"]
    images = {}
    for service in services:
        manifest = json.loads((manifests / f"{service['service']}.json").read_text())
        repository = (
            "ghcr.io/"
            + os.environ["GITHUB_REPOSITORY"].lower()
            + service["image-suffix"]
        )
        if manifest["repository"] != repository or manifest["revision"] != revision:
            raise ValueError("published image does not match the release source")
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", manifest["digest"]):
            raise ValueError("published image lacks an immutable digest")
        images[repository] = manifest
    root = Path("infra/argocd/app-of-apps")
    applications = yaml.safe_load((root / "values.yaml").read_text())["applications"]
    release = {"global": {"targetRevision": revision}, "applications": {}}
    for name, application in applications.items():
        chart = Path(application["path"])
        values = yaml.safe_load((chart / "values.yaml").read_text())
        overrides = image_values(values, images)
        if overrides:
            release["applications"][name] = {"releaseValues": overrides}
    branch = f"deploy/{environment}"
    previous = git("ls-remote", "origin", f"refs/heads/{branch}").split()
    git("switch", "--force-create", branch, revision)
    (root / "release.yaml").write_text(yaml.safe_dump(release, sort_keys=False))
    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    git("add", str(root / "release.yaml"))
    git("commit", "-m", f"Deploy {environment} from {revision}")
    git(
        "push",
        f"--force-with-lease=refs/heads/{branch}:{previous[0] if previous else ''}",
        "origin",
        f"HEAD:refs/heads/{branch}",
    )


if __name__ == "__main__":
    main()
