# Bulk-command e2e smokes

Five command smokes (`fill`, `update`, `delete`, `copy`, `diff`) running
against transient DynamoDB tables created and torn down per test.

## How to run

```sh
make test-e2e-commands
```

~10-15 min, ~\$0 (cold-start dominated for tiny tables; idle
PAY_PER_REQUEST tables are free).

## What gets verified

| Command | Setup | Asserts |
|---|---|---|
| `fill`   | empty transient table | exit 0 + perf captured |
| `update` | seed via fill, then update with `touched` generator | exit 0 |
| `delete` | seed via fill, then delete with `where pk is not null` | exit 0 |
| `copy`   | two transient tables, src seeded | exit 0 (same-region copy) |
| `diff`   | two transient tables, both seeded | exit 0 |

Each command's output is captured, perf is appended to the Command Smoke
Report at suite end, and the transient tables are deleted in the test's
`finally` block. The suite never touches your existing DynamoDB tables.

## What's NOT covered (followup PRs)

- `load-export` — needs a pre-existing DynamoDB export prefix. Either
  supplied via config or auto-triggered (~10-15 min). Will land as
  `make test-e2e-commands-export`.
- `copy --source <ARN>` cross-region/cross-account. Needs IAM role
  wiring on both sides. Will land as `make test-e2e-commands-multi-account`.
- `diff` across regions. Same.
- `scancount` — bypasses the connector by design (direct boto3 scan),
  doesn't exercise the wrapper path PR #162 changed.

See `specs/e2e-commands.md` for design rationale.

## Failure recovery

The transient-table context manager destroys the table on exit even when
the test fails. The one risk is a hard kill (`kill -9`) of the test
runner between table-create and the `finally` block — that leaves an
orphan table.

Orphans are tagged for easy discovery:

```sh
aws dynamodb list-tables --query 'TableNames[?starts_with(@, `bulk-e2e-`)]'
```

Anything matching `bulk-e2e-{label}-{8-hex-suffix}` is from this suite
and safe to delete.
