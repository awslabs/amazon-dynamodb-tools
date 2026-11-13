// Starts ExtendDB for the test run unless the environment already supplies one.
//
// In CI with a service container, DDB_ENDPOINT is set and this does nothing but
// pass it through. On a laptop with nothing running, it starts an in-memory
// server process on an ephemeral port for the test run (no container, no
// daemon; measured at about 190 ms on an Apple-silicon laptop) and stops it
// afterwards.
import { start } from "@extenddb/dev";

const EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
const EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

export default async function setup({ provide }) {
  if (process.env.DDB_ENDPOINT) {
    provide("ddb", {
      endpoint: process.env.DDB_ENDPOINT,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? EXAMPLE_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? EXAMPLE_SECRET,
      // Must match the server's configured region: SigV4 includes the region in
      // the credential scope, so a mismatch surfaces as InvalidSignatureException.
      region: process.env.AWS_REGION ?? "us-east-1",
    });
    return;
  }

  // Here server.credentials is the pair this server reports, which is the example
  // pair, and AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not used.
  // Setting EXTENDDB_DEV_ACCESS_KEY_ID / EXTENDDB_DEV_SECRET_ACCESS_KEY makes
  // the server seed that pair in addition to the example pair, which is always
  // seeded. start() still reports the example pair in server.credentials, so to
  // sign with a custom key, pass it to the client explicitly.
  const server = await start({ memory: true });
  provide("ddb", {
    endpoint: server.endpoint,
    accessKeyId: server.credentials.accessKeyId,
    secretAccessKey: server.credentials.secretAccessKey,
    region: server.region,
  });
  return async () => {
    await server.stop();
  };
}
