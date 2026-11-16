// Starts ExtendDB for the test run unless the environment already supplies one.
//
// In CI with a service container, DDB_ENDPOINT is set and this only passes it
// through. Otherwise it spawns an in-memory ExtendDB child process on an
// ephemeral port for the test run, and stops it afterwards. That start took
// about 190 ms on an Apple-silicon laptop.
import { start } from "@extenddb/dev";

const EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
const EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

export default async function setup({ provide }) {
  if (process.env.DDB_ENDPOINT) {
    // Only use the environment pair if both halves are set.
    const keyId = process.env.AWS_ACCESS_KEY_ID;
    const secret = process.env.AWS_SECRET_ACCESS_KEY;
    const pair = keyId && secret
      ? { accessKeyId: keyId, secretAccessKey: secret }
      : { accessKeyId: EXAMPLE_KEY_ID, secretAccessKey: EXAMPLE_SECRET };
    provide("ddb", {
      endpoint: process.env.DDB_ENDPOINT,
      ...pair,
      // SigV4 signs the region, so it has to match the server's. A mismatch
      // fails with InvalidSignatureException.
      region: process.env.AWS_REGION ?? "us-east-1",
    });
    return;
  }

  // server.credentials is always the example pair. AWS_ACCESS_KEY_ID and
  // AWS_SECRET_ACCESS_KEY are not used on this path.
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
