# Rule: every command that touches DynamoDB must be rate limited

**Why this can't be a unit test:** whether a code path is "rate limited" is a
judgment about how DynamoDB access is wired, not a value you can assert on. Easy
to see by reading; very hard to test mechanically.

## Scope — discover the verbs, don't assume a fixed list

Do **not** work from a hard-coded list of commands. Enumerate the verbs live, so
a newly added command is automatically covered:

- Server-side verbs live under `server/src/python_modules/` — each is either a
  `<verb>.py` module or a `<verb>/__init__.py` package with a `run(...)` entry
  point. Treat every such verb as in scope. (`shared/` is a library, not a verb.)
- Cross-check against the client-side verb dispatch (the CLI's list of commands,
  e.g. in `client/src/runner.py` and `client/src/python_modules/`) so you don't
  miss a verb that exists on one side.

If you find a verb that isn't in any prior version of this rule's reasoning,
that's expected — check it like the rest.

## The invariant

Every verb that reads from or writes to DynamoDB must apply rate limiting via one
of two accepted mechanisms:

1. **Connector-based throttling** — if it reads/writes through the Glue DynamoDB
   DataFrame connector, it must set the connector's throughput-control options
   (e.g. `dynamodb.throughput.read.percent` / `dynamodb.throughput.write.percent`
   or the connector's read/write throughput parameters). Handing the connector no
   throughput bound is a violation.

2. **Our own rate-limiter library** — `python_modules.shared.rate_limiter`
   (`RateLimiterSharedConfig`, `RateLimiterAggregator`, `RateLimiterWorker`). A
   verb hitting DynamoDB directly (boto3 `scan` / `BatchWriteItem` / etc. outside
   the connector) must route that access through a `RateLimiterWorker` session,
   with the throughput target derived from the job's monitor/throughput options.

## What to check, per verb

- **Does it touch DynamoDB at all?** If access is fully delegated to a shared
  helper (e.g. an export writer/pipeline under `shared/export/...`) that is
  itself rate limited, that satisfies the rule — say so and name the helper. A
  verb that never touches DynamoDB (pure formatting/orchestration) is out of
  scope — say so.
- **If it does, which mechanism?** Confirm you can point to either connector
  throughput options being set, or a `RateLimiterWorker`/aggregator/shared-config
  governing the access.
- **Is the whole path covered?** A metadata `describe`/single `scan` for setup is
  fine unbounded, but a bulk data scan/write that bypasses the limiter is a
  violation even if another path in the same verb is limited.

## How to report

For each verb, state one of: `rate-limited via connector`,
`rate-limited via rate_limiter library`, `rate-limited via <named helper>`,
`no DynamoDB access (out of scope)`, or a **finding** naming the specific
function and unbounded DynamoDB call. Only the last is a violation.
