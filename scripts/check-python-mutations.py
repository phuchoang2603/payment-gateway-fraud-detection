#!/usr/bin/env python3
import ast
import fnmatch
import json
import os
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def functions(source):
    result = {}
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            result[node.name] = ast.dump(node)
        elif isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    result[f"{node.name}*{method.name}"] = ast.dump(method)
    return result


def check_project(project, config, base, reports):
    patterns = []
    changed = git(
        "diff", "--name-only", "--diff-filter=ACMR", base, "HEAD", "--", str(project)
    ).splitlines()
    for name in changed:
        path = Path(name)
        relative = path.relative_to(project).as_posix()
        if path.suffix != ".py" or not any(
            fnmatch.fnmatch(relative, p) for p in config.get("only_mutate", ["*.py"])
        ):
            continue
        if not any(
            Path(relative).is_relative_to(root) for root in config["source_paths"]
        ):
            continue
        if any(fnmatch.fnmatch(relative, p) for p in config.get("do_not_mutate", [])):
            continue
        previous = git("ls-tree", "--name-only", base, "--", name)
        old = functions(git("show", f"{base}:{name}")) if previous else {}
        module = relative.removesuffix(".py").replace("/", ".").removeprefix("src.")
        for function, definition in functions((ROOT / path).read_text()).items():
            if old.get(function) != definition:
                patterns.append(f"{module}.*{function}*")
    report = reports / project.relative_to("src")
    report.mkdir(parents=True, exist_ok=True)
    (report / "scope.json").write_text(json.dumps(patterns, indent=2) + "\n")
    if not patterns:
        print(
            f"Skipped {project}: no changed functions in configured mutation scope.",
            flush=True,
        )
        return
    print(f"Mutating {project}: {patterns}", flush=True)
    cwd = ROOT / project
    subprocess.run(
        ["uv", "run", "--locked", "mutmut", "run", "--max-children", "2", *patterns],
        cwd=cwd,
        check=True,
    )
    subprocess.run(
        ["uv", "run", "--locked", "mutmut", "export-cicd-stats"], cwd=cwd, check=True
    )
    with (report / "results.txt").open("w") as output:
        subprocess.run(
            ["uv", "run", "--locked", "mutmut", "results"],
            cwd=cwd,
            stdout=output,
            check=True,
        )
    stats = json.loads((cwd / "mutants/mutmut-cicd-stats.json").read_text())
    (report / "mutation.json").write_text(json.dumps(stats, indent=2) + "\n")
    outcomes = (
        "killed",
        "survived",
        "no_tests",
        "skipped",
        "suspicious",
        "timeout",
        "check_was_interrupted_by_user",
        "segfault",
    )
    evaluated = sum(stats[name] for name in outcomes)
    score = 100 * stats["killed"] / evaluated if evaluated else 0
    summary = f"{project}: {stats['killed']}/{evaluated} mutants killed ({score:.2f}%)"
    print(summary, flush=True)
    if target := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(target).open("a") as output:
            output.write(summary + "\n\n")
    if score <= 80:
        raise SystemExit("Mutation score must exceed 80%; inspect surviving mutants")


def main():
    base = git("merge-base", os.environ["MUTATION_BASE"], "HEAD")
    reports = Path(os.environ["CI_TEST_REPORTS"]).resolve()
    for name in git("ls-files", "--", "src/**/pyproject.toml").splitlines():
        manifest = ROOT / name
        config = tomllib.loads(manifest.read_text()).get("tool", {}).get("mutmut")
        if config:
            check_project(Path(name).parent, config, base, reports)


if __name__ == "__main__":
    main()
