# Test DynamoDB applications against ExtendDB in CI

CI templates and test harnesses for running DynamoDB tests against
[ExtendDB](https://extenddb.org) instead of DynamoDB Local. They cover
TypeScript, Python and Java, on GitHub Actions or GitLab CI.

ExtendDB is a DynamoDB-wire-compatible server. Your SDK code does not change;
you point the endpoint at ExtendDB and handle three behavioural differences,
described below.

## Why

Measured with ExtendDB 0.1.14 against `amazon/dynamodb-local:latest`, on an
arm64 laptop:

| | ExtendDB | DynamoDB Local |
|---|---|---|
| image | 55.4 MB | 571 MB |
| idle memory | 5.9 MB | 174.5 MB |
| container ready | 536 ms | 803 ms |
| runtime | no JVM needed | JVM |

On GitHub Actions, with warm caches, job time is 11 s for the
TypeScript launcher job (no container), and 22 s, 24 s and 34 s for the
TypeScript, Python and Java container jobs, including setup. The GitLab template is verified on
GitLab CI: TypeScript 24 s, Python 27 s, Java 53 s.

## The swap

For most projects this is the whole migration. In a GitHub Actions job, change
the service container and the endpoint.

Before:

```yaml
services:
  dynamodb:
    image: amazon/dynamodb-local:latest
    ports:
      - 8000:8000
env:
  DDB_ENDPOINT: http://localhost:8000
```

After:

```yaml
services:
  dynamodb:
    image: extenddb/extenddb-dev:0.1.14
    ports:
      - 18080:18080
    env:
      EXTENDDB__STORAGE__SQLITE__PATH: ":memory:"
env:
  DDB_ENDPOINT: http://localhost:18080
  AWS_REGION: us-east-1
  AWS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE
  AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Do not add a `--health-cmd` to the ExtendDB container. The image is distroless
and has no shell, so a shell-form health command exits 1 forever and the
container never becomes healthy. The image carries its own HEALTHCHECK; GitHub
Actions waits for it (about 9 s) before the first step runs. GitLab CI does not
wait, so its template includes a bounded wait loop.

## Three things that differ from DynamoDB Local

### 1. `CreateTable` returns `CREATING`

ExtendDB behaves like the real service: a new table is `CREATING` for a short
time. DynamoDB Local returns `ACTIVE` at once. Wait for the table with your
SDK's own waiter, which is correct against both:

- JavaScript v3: `waitUntilTableExists({ client, maxWaitTime: 30, minDelay: 1, maxDelay: 2 }, { TableName })`
- Python: `client.get_waiter("table_exists").wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})`
- Java v2: `db.waiter().waitUntilTableExists(req, o -> o.maxAttempts(30).backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofSeconds(1))))`

The waiters' default minimum delay is 20 s (it comes from the DynamoDB waiter
spec), which is far too slow for a local test, so lower it as the harnesses do.

### 2. Secondary index reads can lag writes

- **Global** secondary index reads are eventually consistent, and GSIs never
  support `ConsistentRead`. On ExtendDB the lag is about 10 ms by default. Real
  DynamoDB is allowed to lag too, so retry rather than assume. The harnesses'
  `eventually` helper does this.
- **Local** secondary index reads are strongly consistent only when you pass
  `ConsistentRead`. Without it they are eventually consistent on real
  DynamoDB. All three harnesses pass it.

### 3. SigV4 is verified

DynamoDB Local accepts any credentials. ExtendDB verifies the signature.

- A known access key with the wrong secret gives `InvalidSignatureException`.
- An unknown access key id gives `UnrecognizedClientException`.
- `AWS_REGION` must match the server's region, because the region is part of
  the signature's credential scope.

The server always seeds AWS's documented example pair
(`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`), so use
that in CI. Real credentials left in your shell are sent to the server and
rejected.

## Determinism, opt-in

Two server settings model the real service's delays: `control_plane_delay_seconds`
(table creation) and `index_propagation_delay_ms` (index reads). Setting both to
`0` through the management API makes tables `ACTIVE` immediately and index reads
immediate, which is stricter than DynamoDB Local. Measured: 0 of 25 stale index
reads, against 25 of 25 with the defaults.

This recipe is for a local container you started yourself. Capture the
password from the container's startup log, then make the two calls:

```bash
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:0.1.14
for i in $(seq 60); do
  [ "$(podman inspect -f '{{.State.Health.Status}}' edb 2>/dev/null)" = healthy ] && ok=1 && break
  sleep 0.5
done
PW=$(podman logs edb 2>&1 | awk '/Password:/ {print $2}' | head -1)
if [ -n "$ok" ] && [ -n "$PW" ]; then
  curl -u admin:$PW -X PUT -H 'Content-Type: application/json' -d '{"value":"0"}' http://127.0.0.1:18080/management/settings/control_plane_delay_seconds
  curl -u admin:$PW -X PUT -H 'Content-Type: application/json' -d '{"value":"0"}' http://127.0.0.1:18080/management/settings/index_propagation_delay_ms
else
  echo "ExtendDB did not become healthy, or no admin password was found; check 'podman logs edb'" >&2
fi
```

An in-memory server's admin password is random and printed to stderr at
startup, which is what the `PW=` line above reads from the container's log. The
recipe applies to a container you started yourself.

We recommend keeping the defaults. They make eventual-consistency bugs show up
on your laptop and in CI instead of in production.

## Running it

- **GitHub Actions:** [`.github/workflows/extenddb-local-testing.yml`](../../.github/workflows/extenddb-local-testing.yml)
  is a live example with two jobs. `container` runs a TypeScript, Python and
  Java matrix with ExtendDB as a service container. `launcher` is TypeScript
  only and has no `services:` block and no container: the npm launcher `@extenddb/dev` spawns the native
  ExtendDB binary as a child process on an ephemeral port, so parallel runs
  cannot collide. The `container` job's service block is:

  ```yaml
  services:
    dynamodb:
      image: extenddb/extenddb-dev:0.1.14
      ports:
        - 18080:18080
      env:
        EXTENDDB__STORAGE__SQLITE__PATH: ":memory:"
  ```

- **GitLab CI:** copy [`gitlab-ci.example.yml`](gitlab-ci.example.yml). It is
  verified on GitLab CI. The service is reached by its alias (`dynamodb`), not
  `localhost`, and a wait loop (60 attempts) replaces the health wait GitHub
  provides.

The harnesses share one contract: use `DDB_ENDPOINT` if the environment
supplies one, otherwise start a server. The same test code therefore runs under
both flavours.

## Per-language notes

- [`harness/typescript`](harness/typescript/README.md): Vitest. With no
  `DDB_ENDPOINT` it starts the server through `@extenddb/dev`. Needs Node 22.12
  or later. Run `npm install && npm test` from
  `beta/extenddb-local-testing/harness/typescript`.
- [`harness/python`](harness/python): pytest with boto3. With no
  `DDB_ENDPOINT` it starts the container with `docker` or `podman`. Run
  `pip install -r requirements.txt && python -m pytest -q`, from
  `beta/extenddb-local-testing/harness/python`.
- [`harness/java`](harness/java): JUnit with AWS SDK v2. Run `mvn -q test` from
  `beta/extenddb-local-testing/harness/java`.
  With no `DDB_ENDPOINT` (local runs only) it starts the container through Testcontainers, which worked with no
  extra variables on a machine where `/var/run/docker.sock` points at the
  podman socket. Where that link does not exist, set
  `DOCKER_HOST=unix://<podman socket path>`, and if Ryuk misbehaves, set
  `TESTCONTAINERS_RYUK_DISABLED=true`.

To run any harness against a container you started yourself, set
`DDB_ENDPOINT` as in the TypeScript README.

## Scope

- Classic DynamoDB API only. PartiQL is not covered.
- Pinned to ExtendDB 0.1.14. To bump it, change the tag in the workflow, in the
  GitLab template and in the harness defaults (`conftest.py`,
  `ExtendDbSupport.java`, and the TypeScript README), and the `@extenddb/dev`
  version in `harness/typescript/package.json` and its lockfile. A monthly
  scheduled run of the workflow catches a release that breaks these templates.

## About ExtendDB

ExtendDB is an independent open source project managed by Amazon Web Services.
It is not Amazon DynamoDB and does not contain any DynamoDB source code.
"DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room
implementation that speaks the DynamoDB wire protocol. It is licensed under
Apache-2.0. More at [extenddb.org](https://extenddb.org) and
[github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb).
