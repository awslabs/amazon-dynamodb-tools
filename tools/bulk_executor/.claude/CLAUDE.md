# Fork-Based PR Workflow

This project uses a two-tier PR model with strict upstream hygiene.

## Repos

- **Fork:** `relentlesscol/amazon-dynamodb-tools` — integration branch is `main`
- **Upstream:** `awslabs/amazon-dynamodb-tools` — the public repo

## Rules

1. **Feature branches → fork PRs only.** Every `polecat/*` branch targets `fork/main`. Each PR shows an isolated diff of just that change.
2. **Max 3 upstream PRs open at a time.** Each must be independently mergeable in any order — no ordering dependencies between them.
3. **Upstream PRs are grouped by domain** (e.g., "server-side fixes", "client-side improvements", "CI + tests"). Cherry-pick from fork/main onto origin/main.
4. **Never open an upstream PR from a feature branch.** This drags all stacked ancestors into the diff.
5. **Label:** Always add `bulk_executor` label to upstream PRs.

## Branching

```
origin/main (awslabs)
  ↑ max 3 PRs, independently mergeable, cherry-picked by domain
  │
fork/main (relentlesscol) ← accumulates merged feature PRs
  ↑ unlimited individual PRs (isolated diffs)
  │
polecat/bu-* (feature branches, forked from fork/main)
```

## Upstream PR requirements

- [ ] Max 3 open at once
- [ ] No ordering dependency — any can merge first
- [ ] Cherry-picked from fork/main (not from feature branches)
- [ ] Grouped by domain with clear summary listing each change
- [ ] Diff only shows files relevant to that domain

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
