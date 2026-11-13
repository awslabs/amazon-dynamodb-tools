// Starts ExtendDB for the test run unless the environment already supplies one.
//
// In CI with a service container, DDB_ENDPOINT is set and this does nothing but
// pass it through. On a laptop with nothing running, it starts an in-memory
// server in-process on an ephemeral port (~190 ms) and stops it afterwards.
import { start } from "@extenddb/dev";

const EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
const EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

export default async function setup({ provide }) {
  if (process.env.DDB_ENDPOINT) {
    provide("ddb", {
      endpoint: process.env.DDB_ENDPOINT,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? EXAMPLE_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? EXAMPLE_SECRET,
      region: process.env.AWS_REGION ?? "us-east-1",
    });
    return;
  }

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
