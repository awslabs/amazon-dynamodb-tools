# Fork-Based PR Workflow

This project uses a two-tier PR model. **Never open upstream PRs from feature branches.**

## Repos

- **Fork:** `relentlesscol/amazon-dynamodb-tools` — integration branch is `main`
- **Upstream:** `awslabs/amazon-dynamodb-tools` — the public repo

## Rules

1. **Feature branches → fork PRs only.** Every `polecat/*` branch targets `fork/main`. Each PR shows an isolated diff of just that change.
2. **Fork main → upstream PRs.** When a batch of fork PRs has merged into `fork/main`, open ONE upstream PR from `fork/main` (or a branch cut from it) targeting `origin/main`.
3. **Never open an upstream PR from a feature branch.** This drags all stacked ancestors into the diff. If you catch yourself doing `--head relentlesscol:polecat/...` against `awslabs`, stop.
4. **Upstream PR naming:** `[bulk_executor] batch: <summary>` with a body listing each included change.
5. **Label:** Always add `bulk_executor` label to upstream PRs.

## Branching

```
origin/main (awslabs)
  └── fork/main (relentlesscol) ← accumulates merged feature PRs
        ├── polecat/bu-xyz (feature) → PR to fork/main
        ├── polecat/bu-abc (feature) → PR to fork/main
        └── upstream-batch-N → PR to origin/main (cut from fork/main)
```

## Checklist before opening any PR

- [ ] Target is correct (`fork/main` for features, `origin/main` for batches)
- [ ] Diff only shows THIS change's files (not ancestors)
- [ ] Branch was forked from the correct base (not from another feature branch tip)

---

# Skills

## Run Unit Tests

After completing a logical set of changes, run the unit tests to verify nothing is broken.

All python commands must activate the local virtual environment first.

### Steps

From the `tools/bulk_executor/` directory:

```bash
source .venv/bin/activate && make install && make test
```

### When to run

- After completing a cohesive set of related changes (not after every single edit)
- Before reporting that a task is complete
- After refactoring or modifying existing functionality

### Notes

- Use `make test-client` or `make test-server` if changes are scoped to only one side
- If tests fail, fix the issues before moving on
