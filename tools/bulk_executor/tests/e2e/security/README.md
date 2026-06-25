# Security e2e suite

Validates that the README-documented bootstrap IAM policy is **complete** (no
missing actions) and **minimal** (every Sid is load-bearing). Two tiers:

| Tier | What it does | Cost | Time | Side effects |
|---|---|---|---|---|
| Simulator | Runs `iam:SimulateCustomPolicy` per documented action — no resources created | $0 | ~30s | None |
| Real | Creates a temp IAM user, attaches the README policy, runs real `bulk bootstrap` + `bulk teardown` | ~$0 | ~3 min | **Destructive: tears down the shared `bulk_dynamodb` Glue job** |

## How to run

```sh
make test-e2e-security              # Both tiers (simulator first; real only runs if simulator passes)
make test-e2e-security-simulator    # Tier 1 only — fast, safe to run anytime
make test-e2e-security-real         # Tier 2 only — destructive, see warning below
```

## ⚠️ Tier 2 is destructive against shared infrastructure

The real-bootstrap test runs `bulk bootstrap --XRole READ-ONLY` and then
`bulk teardown` on the shared `bulk_dynamodb` Glue job — the same job the
connector smoke suite uses. **Do not run tier 2 in parallel with the
connector smoke suite, or against a production account.**

If you run tier 2 and then immediately try to run the connector smoke suite,
expect failures: the Glue execution role's managed policies will have been
detached, and `bulk bootstrap` is not idempotent on policy state — it only
re-attaches policies when *creating* the role, not when re-bootstrapping
into an existing one.

Recovery: run `bulk teardown` then `bulk bootstrap` to fully rebuild.

**Followup**: a `--XJobName` override on bootstrap/teardown would let this
suite use a dedicated `bulk_dynamodb_security` Glue job and stop touching
the shared one. Tracked as a known-followup in PR #162.

### Random-negative test can leave orphan Glue roles

The `test_random_action_removal_breaks_bootstrap` test removes one action
and expects bootstrap to fail. Bootstrap is not atomic — it may create the
Glue execution role *before* hitting the missing-permission denial. When
that happens, the temp IAM user is cleaned up by the context manager, but
the orphan `AWSGlueServiceRoleBulkDynamoDB-*` role survives.

Recovery: list roles matching `AWSGlueServiceRoleBulkDynamoDB-*`, identify
any with `CreateDate` after this run started, and delete them (detach
managed policies + delete inline policies first).

Followup: have the negative test run `bulk teardown` with the original
admin creds inside its `finally` block to scrub partial bootstrap state.

## What each test asserts

### `test_simulator.py::test_full_policy_allows_every_documented_action`

Every action listed in the README policy must evaluate as `allowed` against
the policy. If this fails, the README has a typo, a misspelled action, or a
resource scope too narrow to authorize the documented action.

### `test_simulator.py::test_removing_statement_denies_at_least_one_action[<sid>]`

Removing each Sid from the policy must cause at least one of its actions to
become denied. If this fails, the Sid is decorative — either over-specified
or another Sid grants the same actions.

### `test_real_bootstrap.py::test_documented_policy_can_actually_bootstrap`

Creates a temp IAM user with the README policy attached, invokes
`bulk bootstrap --XRole READ-ONLY` with those credentials, asserts exit 0,
runs teardown, asserts exit 0. If bootstrap fails, the README is incorrect
— it claims a permission set that doesn't actually let bootstrap complete.
The error output names the missing action(s) for the followup README fix.

This tier is the truth oracle for the simulator: simulator passes mean the
*documented* policy is consistent; real-bootstrap passes mean the simulator
is correctly modeling reality.

## Why we keep both tiers

The simulator catches drift fast and free. The real test catches everything
the simulator misses (service-side validation, eventual-consistency
weirdness, runtime conditions that aren't expressible as IAM policy). When
they disagree, we have a Glue/IAM gap worth surfacing.
