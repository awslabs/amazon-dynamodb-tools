# TypeScript harness

Runs the shared suite against ExtendDB with Vitest. Requires Node.js 22.12 or
later.

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
of starting anything. This is what the CI service-container flavour does. With
no `DDB_ENDPOINT` set, `test:container` defaults to `http://127.0.0.1:18080`.

```bash
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:0.1.14
npm run test:container
podman rm -f edb
```

## Credentials

The harness signs requests with SigV4, and ExtendDB verifies the signature, so
the credentials must match a pair the server seeds. The example pair
`AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` is always
seeded by the dev image. Against a running server, the harness uses
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if you set them, and otherwise
the example pair. When the harness starts its own server, it uses the example
pair. Unlike DynamoDB Local, which accepts any credentials, ExtendDB rejects a
pair it did not seed.
