"""Prepare docs/ for the GitHub wiki, which serves every page at its root by file name.

- Pages that share a file name (llm/observability.md and deployment/observability.md, each product's
  payment-events.md) would collide, so each is renamed with its folder as a prefix and the links to it follow.
- Other files keep their nested paths, so a page's relative `images/x.png` would resolve under the wiki root and
  break: images point at the wiki repository's raw files, other files (HTML reports) at the main repository.

Usage: python scripts/wiki_links.py docs owner/repo
"""

import re
import sys
from collections import Counter
from pathlib import Path

LINK = re.compile(r"(!?\[[^\]]*\]\()([^)\s]+)((?:\s+\"[^\"]*\")?\))")
IMAGES = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"}


def unique_names(root: Path) -> dict[Path, Path]:
    """New paths for pages whose file name another page also has."""
    pages = sorted(root.rglob("*.md"))
    counts = Counter(page.name for page in pages)
    return {
        page: page.with_name(f"{page.parent.name}-{page.name}")
        for page in pages
        if counts[page.name] > 1
    }


def rewrite(page: Path, root: Path, repo: str, renamed: dict[Path, Path]) -> str:
    def target(match: re.Match) -> str:
        path, _, anchor = match.group(2).partition("#")
        if not path or "://" in path or path.startswith(("mailto:", "/")):
            return match.group(0)
        resolved = (page.parent / path).resolve()
        if not resolved.is_relative_to(root) or not resolved.is_file():
            return match.group(0)
        suffix = f"#{anchor}" if anchor else ""
        if resolved.suffix == ".md":
            if resolved not in renamed:
                return match.group(0)
            new = (
                renamed[resolved]
                .relative_to(renamed.get(page, page).parent, walk_up=True)
                .as_posix()
            )
            return f"{match.group(1)}{new}{suffix}{match.group(3)}"
        relative = resolved.relative_to(root).as_posix()
        if resolved.suffix.lower() in IMAGES:
            url = f"https://raw.githubusercontent.com/wiki/{repo}/{relative}"
        else:
            url = f"https://github.com/{repo}/blob/main/{root.name}/{relative}"
        return f"{match.group(1)}{url}{suffix}{match.group(3)}"

    return LINK.sub(target, page.read_text())


def main(root: str, repo: str) -> int:
    base = Path(root).resolve()
    renamed = unique_names(base)
    texts = {
        page: rewrite(page, base, repo, renamed) for page in sorted(base.rglob("*.md"))
    }
    for page, text in texts.items():
        if text != page.read_text():
            page.write_text(text)
    for old, new in renamed.items():
        old.rename(new)
        print(f"renamed {old.relative_to(base)} -> {new.relative_to(base)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
