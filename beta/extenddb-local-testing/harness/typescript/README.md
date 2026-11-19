# TypeScript harness

Runs the shared suite against ExtendDB with Vitest. Requires Node.js 22.12 or later.

Run every command below from this directory. From the root of the repository checkout:

```bash
cd beta/extenddb-local-testing/harness/typescript
```

## Two ways to get a server

### Self-started, with no container

This is the default. `globalSetup.mjs` calls `start({ memory: true })` from `@extenddb/dev`, which spawns the native ExtendDB binary as a child process on an ephemeral port for the test run and stops it afterwards.

```bash
npm install
npm test
```

The ephemeral port keeps the run clear of any other server on the machine. The in-memory server started in about 190 ms on an Apple-silicon laptop. Vitest's verbose reporter, set in `vitest.config.mjs`, lists each test by name.

### Against a running server

Set `DDB_ENDPOINT` and the harness uses it instead of starting anything. A CI service container works this way. With no `DDB_ENDPOINT` set, `npm run test:container` defaults to `http://127.0.0.1:18080`. The script uses POSIX shell syntax. On Windows `cmd`, set `DDB_ENDPOINT` yourself and run `npm test`.

The harness does not wait or retry, so wait for the container first. The sequence below waits up to 30 s for `/health` to answer 200 and runs the tests only then. Podman ignores the image's HEALTHCHECK, so the loop asks the server over HTTP. It always removes the container, so a failed attempt does not block the next one. `docker` works the same.

```bash
podman rm -f edb >/dev/null 2>&1
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:1.0.0
ok=
for i in $(seq 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/health)" = 200 ] && ok=1 && break
  sleep 0.5
done
if [ -n "$ok" ]; then
  npm run test:container
else
  echo "edb did not answer on /health within 30 s. Check 'podman logs edb' and that port 18080 is free." >&2
fi
podman rm -f edb
```

## Credentials

ExtendDB verifies each request's SigV4 signature, so the harness has to sign with a pair the server seeds. By default the server seeds the example pair `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`. ExtendDB rejects credentials it did not seed. DynamoDB Local accepts any.

- Against a running server, the harness uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if you set both, and otherwise the example pair. Real AWS credentials left in your shell are therefore sent to the server. It does not know that key id, so the run fails with `UnrecognizedClientException`. Unset them, or export the example pair, before `npm run test:container`.
- When the harness starts its own server, it always signs with the example pair and ignores `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- On the `DDB_ENDPOINT` path the region comes from `AWS_REGION` and defaults to `us-east-1`. ExtendDB rejects a request signed for a region other than `us-east-1` with `InvalidSignatureException`, as real DynamoDB rejects one signed for the wrong region.
