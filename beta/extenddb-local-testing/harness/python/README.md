# Python harness

Runs the shared suite against ExtendDB with pytest and boto3.

Run every command below from this directory. From the root of the repository checkout:

```bash
cd beta/extenddb-local-testing/harness/python
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Two ways to get a server

### Self-started container

With no `DDB_ENDPOINT`, `conftest.py` starts `extenddb/extenddb-dev:1.0.0` on a free local port with `docker`, or with `podman` if `docker` is not installed. It waits until the server answers and removes the container at the end of the session. If neither runtime is installed, the run fails with a message instead of skipping.

```bash
python -m pytest -v
```

`-v` lists each test by name.

### Against a running server

Set `DDB_ENDPOINT` and the harness uses it instead of starting a container. It waits up to 60 s for the server to answer, so you can start the tests right after the container. `docker` works the same as `podman` here.

```bash
podman rm -f edb >/dev/null 2>&1
podman run -d --name edb -p 127.0.0.1:18080:18080 \
  -e EXTENDDB__STORAGE__SQLITE__PATH=:memory: extenddb/extenddb-dev:1.0.0
DDB_ENDPOINT=http://127.0.0.1:18080 python -m pytest -v
podman rm -f edb
```

## Credentials

ExtendDB verifies each request's SigV4 signature, so the harness has to sign with a pair the server seeds. By default the server seeds the example pair `AKIAIOSFODNN7EXAMPLE` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`.

- The harness uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if both are set, and otherwise the example pair. That holds on every path, including a container the harness starts itself.
- Real AWS credentials left in your shell are rejected. The run stops at once with a message that names the error (`UnrecognizedClientException` for an unknown key id). Unset them, or export the example pair.
- The region is fixed at `us-east-1`, the server's default.
