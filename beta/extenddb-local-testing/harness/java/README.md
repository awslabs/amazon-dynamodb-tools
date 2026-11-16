# Java harness

Runs the shared suite against ExtendDB with JUnit and the AWS SDK for Java v2. Requires JDK 17 or later and Maven.

Run every command below from this directory. From the root of the repository checkout:

```bash
cd beta/extenddb-local-testing/harness/java
```

## Two ways to get a server

### Self-started container

With no `DDB_ENDPOINT`, `ExtendDbSupport` starts `extenddb/extenddb-dev:0.1.14` through Testcontainers and waits up to 60 s for the image's HEALTHCHECK.

```bash
mvn -B -ntp test
```

Surefire prints one line per test, because the POM sets `reportFormat` to `plain`. The POM also sets `failIfNoTests`, so a run that executes zero tests fails.

Testcontainers needs a Docker-compatible socket. It worked with no extra variables on a machine where `/var/run/docker.sock` points at the podman socket. Without that link, point `DOCKER_HOST` at the podman socket. If Ryuk misbehaves, turn it off.

```bash
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
```

### Against a running server

Set `DDB_ENDPOINT` and the harness uses it instead of starting a container. The harness does not wait or retry, so wait for the container first. The sequence below waits up to 30 s for the container's health state, runs the tests only if it is healthy, and always removes the container. `docker` works the same.

```bash
podman rm -f edb >/dev/null 2>&1
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:0.1.14
ok=
for i in $(seq 60); do
  [ "$(podman inspect -f '{{.State.Health.Status}}' edb 2>/dev/null)" = healthy ] && ok=1 && break
  sleep 0.5
done
if [ -n "$ok" ]; then
  DDB_ENDPOINT=http://127.0.0.1:18080 mvn -B -ntp test
else
  echo "edb did not become healthy within 30 s. Check 'podman logs edb' and that port 18080 is free." >&2
fi
podman rm -f edb
```

## Credentials

ExtendDB verifies each request's SigV4 signature, so the harness has to sign with a pair the server seeds. The server always seeds the example pair `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`.

- The harness uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if both are set, and otherwise the example pair. That holds on every path, including a container the harness starts itself.
- Real AWS credentials left in your shell are rejected, and the suite fails in `@BeforeAll` with `UnrecognizedClientException`. Unset them, or export the example pair.
- The region is fixed at `us-east-1`, the server's default.
