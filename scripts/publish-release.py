import json
import os
import re
import subprocess
from pathlib import Path


def git(*arguments):
    return subprocess.check_output(["git", *arguments], text=True).strip()


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
    tag = os.environ["DEPLOY_IMAGE_TAG"]
    if not re.fullmatch(rf"(?:pr-[0-9]+|main)-{revision}", tag):
        raise ValueError("image tag does not match the release source")
    root = Path("infra/argocd/app-of-apps")
    release = {"global": {"targetRevision": revision, "imageTag": tag}}
    branch = f"deploy/{environment}"
    previous = git("ls-remote", "origin", f"refs/heads/{branch}").split()
    git("switch", "--force-create", branch, revision)
    (root / "release.yaml").write_text(json.dumps(release, indent=2) + "\n")
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
