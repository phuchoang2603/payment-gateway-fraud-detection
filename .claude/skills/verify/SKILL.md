---
name: verify
description: Prove a change works in this repo with devenv lint locally, then CI results and test evidence from GitHub Actions.
---

# Verify a change

1. Lint locally inside devenv: `devenv tasks run repo:lint` (ast-grep rules, golangci-lint with depguard, Ruff, import-linter, contracts, Helm).
2. Never run tests locally. CI runs them through `scripts/check.sh`.
3. Push, then read CI: `gh run list --commit <sha>`, `gh run watch <id> --exit-status`, `gh run view <id> --log-failed`.
4. Read test evidence in each job's step summary (`gh run view <id>`) and any `*-test-evidence-*` artifacts with `gh run download <id>`.
5. For deployed services, check Argo CD health through the global `talos-k8s` skill.
