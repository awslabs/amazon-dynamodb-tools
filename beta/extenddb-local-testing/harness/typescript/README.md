# TypeScript harness

Runs the shared suite against ExtendDB with Vitest. Requires Node.js 22.12 or
later.

Run every command below from this directory. From the root of the repository
checkout:

```bash
cd beta/extenddb-local-testing/harness/typescript
```

## Two ways to get a server

**Self-started (no container).** The default. `globalSetup.mjs` calls
`start({ memory: true })` from `@extenddb/dev`, which spawns the native ExtendDB
binary as a child server process on an ephemeral port for the test run, and
stops it afterwards. No container and no daemon are involved.

```bash
npm install
npm test
```

The ephemeral port means the run never collides with another server on the
machine, and the in-memory server is ready in about 190 ms on an Apple-silicon
laptop.

**Against a running server.** Set `DDB_ENDPOINT` and the harness uses it instead
of starting anything. This is the pattern a CI service container uses. With no
`DDB_ENDPOINT` set, `test:container` defaults to `http://127.0.0.1:18080`.

The container takes about 2 s to become healthy, and the harness does not wait
or retry, so wait for the container's health state before running the tests:

```bash
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:0.1.14
until [ "$(podman inspect -f '{{.State.Health.Status}}' edb)" = "healthy" ]; do sleep 0.5; done
npm run test:container
podman rm -f edb
```

## Credentials

The harness signs requests with SigV4, and ExtendDB verifies the signature, so
the credentials must match a pair the server seeds. The example pair
`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` is always
seeded by the server. Unlike DynamoDB Local, which accepts any credentials,
ExtendDB rejects a pair it did not seed.

- Against a running server, the harness uses `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` if you set them, and otherwise the example pair.
  Real AWS credentials left in your shell are therefore sent to the server,
  which does not know that key ID, and the run fails with
  `UnrecognizedClientException`. Unset them, or export the example pair, before
  `npm run test:container`.
- When the harness starts its own server, it always uses the example pair and
  ignores `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- `AWS_REGION` defaults to `us-east-1` on the `DDB_ENDPOINT` path. It must match
  the server's region, because SigV4 puts the region in the credential scope.
