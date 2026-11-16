# Test DynamoDB applications against ExtendDB in CI

CI templates and test harnesses for running DynamoDB tests against [ExtendDB](https://extenddb.org) instead of DynamoDB Local. They cover TypeScript, Python and Java, on GitHub Actions or GitLab CI.

ExtendDB is a server that speaks the DynamoDB wire protocol. Your SDK calls stay the same. You point the endpoint at ExtendDB and handle three behavioral differences, described below.

## Size and startup compared with DynamoDB Local

Measured with ExtendDB 0.1.14 against `amazon/dynamodb-local:latest`, on an arm64 laptop:

| | ExtendDB | DynamoDB Local |
|---|---|---|
| image | 55.4 MB | 571 MB |
| idle memory | 5.9 MB | 174.5 MB |
| container ready (first `list-tables` answered) | 536 ms | 803 ms |
| runtime | no JVM needed | JVM |

The readiness times include AWS CLI startup on each poll, so they measure the time until a real client call succeeds.

## Swapping DynamoDB Local for ExtendDB

For most projects this is the whole migration.

### GitHub Actions

Change the service container and the endpoint in the job.

With DynamoDB Local:

```yaml
services:
  dynamodb:
    image: amazon/dynamodb-local:latest
    ports:
      - 8000:8000
env:
  DDB_ENDPOINT: http://localhost:8000
```

With ExtendDB:

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

The image has its own HEALTHCHECK, and GitHub Actions waits for it before the first step runs. The check runs every 10 s, and the measured wait was about 9 s. Leave `--health-cmd` unset. The image is distroless and has no shell, so a shell-form health command always fails and the container never reports healthy.

### GitLab CI

Copy the top-level `variables:` block and the `.extenddb` job from [`gitlab-ci.example.yml`](gitlab-ci.example.yml) into your `.gitlab-ci.yml`. Then make your test job extend `.extenddb`.

With DynamoDB Local:

```yaml
test:
  image: node:22
  services:
    - name: amazon/dynamodb-local:latest
      alias: dynamodb
  variables:
    DDB_ENDPOINT: "http://dynamodb:8000"
  script:
    - npm ci
    - npm test
```

With ExtendDB:

```yaml
test:
  extends: .extenddb
  image: node:22
  script:
    - npm ci
    - npm test
```

`.extenddb` adds the ExtendDB service under the alias `dynamodb` and a `before_script:` that waits for it. The top-level variables set `DDB_ENDPOINT` to `http://dynamodb:18080` and supply the example credentials. GitLab CI does not wait for the image's HEALTHCHECK, so the wait loop is needed. The template's header lists two pitfalls when you adapt it. A job-level `before_script:` or `services:` replaces the inherited one.

### How your application finds the endpoint

`DDB_ENDPOINT` is a convention of the harnesses in this directory. No SDK reads it. If your tests already pass the DynamoDB Local endpoint through a variable of their own, keep that variable and give it the ExtendDB URL.

The AWS SDKs also read the service-specific variable `AWS_ENDPOINT_URL_DYNAMODB`. A client created with no endpoint in code then talks to ExtendDB, so your client code does not change. This works with all three SDK versions pinned here (`@aws-sdk/client-dynamodb` 3.1169.0, boto3 1.44.3 and AWS SDK for Java 2.59.0).

On GitHub Actions the service is on `localhost`:

```yaml
env:
  AWS_ENDPOINT_URL_DYNAMODB: http://localhost:18080
```

On GitLab CI it is reached by its service alias:

```yaml
variables:
  AWS_ENDPOINT_URL_DYNAMODB: "http://dynamodb:18080"
```

Each SDK needs some region to build a client. boto3 1.44.3 reads it from `AWS_DEFAULT_REGION` and does not read `AWS_REGION`, so for a Python application add this too:

```yaml
AWS_DEFAULT_REGION: us-east-1
```

## Three things that differ from DynamoDB Local

### 1. `CreateTable` returns `CREATING`

A new table is `CREATING` for a short time, as on the real service. DynamoDB Local returns `ACTIVE` at once. Wait for the table with your SDK's waiter, which works against both.

The waiters' default minimum delay is 20 s, from the DynamoDB waiter spec. That is too slow for a local test, so lower it as the harnesses do.

JavaScript (AWS SDK v3):

```javascript
import { waitUntilTableExists } from "@aws-sdk/client-dynamodb";

await waitUntilTableExists({ client, maxWaitTime: 30, minDelay: 1, maxDelay: 2 }, { TableName: name });
```

Python (boto3):

```python
client.get_waiter("table_exists").wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
```

Java (AWS SDK v2):

```java
import java.time.Duration;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;

db.waiter().waitUntilTableExists(
    r -> r.tableName(name),
    o -> o.maxAttempts(30).backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofSeconds(1))));
```

### 2. Secondary index reads can lag writes

- **Global** secondary index reads are eventually consistent, and GSIs never support `ConsistentRead`. On ExtendDB the lag is about 10 ms by default. Real DynamoDB can lag too, so poll until the item appears. The harnesses' `eventually` helper does this.
- **Local** secondary index reads, like base-table reads, are strongly consistent only when you pass `ConsistentRead`. Without it they are eventually consistent on real DynamoDB. All three harnesses pass it for both.

### 3. SigV4 is verified

DynamoDB Local accepts any credentials. ExtendDB verifies the signature.

- A known access key with the wrong secret gives `InvalidSignatureException`.
- An unknown access key id gives `UnrecognizedClientException`.
- ExtendDB 0.1.14 does not check the region a request is signed for. The suite passes with `AWS_REGION=eu-west-1`. Real DynamoDB does check it, so keep your region consistent if you also run against the real service.

The server always seeds AWS's published example pair (`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`), so sign with that pair in CI. On your own machine, unset real `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in your shell, or export the example pair. Each harness README says how that harness picks its credentials and region.

## Turning off the simulated delays

Two server settings model the real service's delays. `control_plane_delay_seconds` covers table creation, and `index_propagation_delay_ms` covers index reads. With both set to `0`, tables are `ACTIVE` at once and index reads are immediate, as on DynamoDB Local. In a measured run, 0 of 25 index reads were stale with both at `0`, against 25 of 25 with the defaults.

Keep the defaults if you can. They make eventual-consistency bugs show up in your own tests.

To change the settings on a container you started yourself, call the management API with the admin password. An in-memory server's admin password is random and printed to stderr at startup, so the `PW=` line below reads it from the container's log. `docker` works the same as `podman` here.

```bash
podman rm -f edb >/dev/null 2>&1
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:0.1.14
ok=
for i in $(seq 60); do
  [ "$(podman inspect -f '{{.State.Health.Status}}' edb 2>/dev/null)" = healthy ] && ok=1 && break
  sleep 0.5
done
PW=$(podman logs edb 2>&1 | awk '/Password:/ {print $2}' | head -1)
if [ -n "$ok" ] && [ -n "$PW" ]; then
  curl -fsS -u "admin:$PW" -X PUT -H 'Content-Type: application/json' -d '{"value":"0"}' http://127.0.0.1:18080/management/settings/control_plane_delay_seconds
  curl -fsS -u "admin:$PW" -X PUT -H 'Content-Type: application/json' -d '{"value":"0"}' http://127.0.0.1:18080/management/settings/index_propagation_delay_ms
else
  echo "ExtendDB did not become healthy, or no admin password was found. Check 'podman logs edb'." >&2
fi
```

## The CI templates

- [`.github/workflows/extenddb-local-testing.yml`](../../.github/workflows/extenddb-local-testing.yml) is a live GitHub Actions example. Two of its jobs are the template. `container` runs a TypeScript, Python and Java matrix with ExtendDB as a service container, using the service block from the swap above. `launcher` is TypeScript only and has no service container. The npm launcher `@extenddb/dev` spawns the native ExtendDB binary as a child process on an ephemeral port, so parallel runs do not clash. The file also has two canary jobs that run monthly against the newest ExtendDB release. They catch a new release that breaks these templates. You do not need to copy them.
- [`gitlab-ci.example.yml`](gitlab-ci.example.yml) is the GitLab CI template to copy. It is verified on GitLab CI.

On GitHub Actions, with warm caches, the TypeScript launcher job takes 11 s. The TypeScript, Python and Java container jobs take 22 s, 24 s and 34 s, including setup. On GitLab CI the jobs take 24 s (TypeScript), 27 s (Python) and 53 s (Java).

Every job lists each test it ran by name.

## Running the harnesses locally

Each harness uses `DDB_ENDPOINT` if the environment sets it, and otherwise starts a server. The same test code runs in both CI jobs. Each harness README shows how to run it against a container you started yourself.

### TypeScript

The suite runs on Vitest and needs Node 22.12 or later. Leave `DDB_ENDPOINT` unset and `@extenddb/dev` starts the server for you. See [`harness/typescript/README.md`](harness/typescript/README.md).

```bash
cd beta/extenddb-local-testing/harness/typescript
npm install
npm test
```

### Python

pytest and boto3. If `DDB_ENDPOINT` is unset, the session fixture runs the container with `docker` or `podman`. See [`harness/python/README.md`](harness/python/README.md).

```bash
cd beta/extenddb-local-testing/harness/python
pip install -r requirements.txt
python -m pytest -v
```

### Java

This one uses JUnit and the AWS SDK for Java v2. Testcontainers starts the container when `DDB_ENDPOINT` is unset. The POM sets `failIfNoTests`, so a run that executes zero tests fails. See [`harness/java/README.md`](harness/java/README.md).

```bash
cd beta/extenddb-local-testing/harness/java
mvn -B -ntp test
```

## Isolating tests

- Give every test its own table name. The harnesses' `tableName` and `table_name` helpers append a unique suffix. This carries over to real DynamoDB and is safe for parallel shards sharing one server.
- Isolating by account is an ExtendDB-only advanced option. It needs admin credentials and does not carry over to real DynamoDB.

## Scope

- Classic DynamoDB API only. PartiQL is not covered.
- A standalone binary download for Python and Java is a possible future option. It is not offered today because it would depend on undocumented package internals.
- Pinned to ExtendDB 0.1.14. Once a month, and on manual dispatch, the repository's own workflow also runs the suite against the newest ExtendDB release (`extenddb/extenddb-dev:latest` and `@extenddb/dev@latest`). A failure there means a new release broke the templates. That canary is repository maintenance and not part of the template you copy.

To bump the version, change the image tag everywhere this search finds it, including this README. Then update the npm launcher, which also rewrites the lockfile.

```bash
NEW_VERSION=x.y.z
grep -rnI --exclude-dir=node_modules --exclude-dir=target 'extenddb-dev:0\.1\.14' beta/extenddb-local-testing .github/workflows/extenddb-local-testing.yml
cd beta/extenddb-local-testing/harness/typescript
npm install --save-exact --save-dev "@extenddb/dev@$NEW_VERSION"
```

## About ExtendDB

ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and does not contain any DynamoDB source code. "DynamoDB" is a trademark of Amazon.com, Inc. ExtendDB is a clean-room implementation that speaks the DynamoDB wire protocol. It is licensed under Apache-2.0. More at [extenddb.org](https://extenddb.org) and [github.com/ExtendDB/extenddb](https://github.com/ExtendDB/extenddb).
