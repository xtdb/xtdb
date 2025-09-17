import { GenericContainer } from "testcontainers";
import { randomBytes } from "crypto";

let container;
const runningInCI = process.env.CI === "true";

export const setupTimeout = 600000;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const overrideSetup = null;
// const overrideSetup = { host: "localhost", port: 5439 }; // uncomment for testing with specific instance

export async function setup() {
  this.timeout(10000);

  if (overrideSetup) {
    process.env.XTDB_HOST = overrideSetup.host;
    process.env.XTDB_PG_PORT = overrideSetup.port.toString();
  } else {
    let containerSetup = new GenericContainer("ghcr.io/xtdb/xtdb:nightly")
      .withEnvironment({
        XTDB_LOGGING_LEVEL_PGWIRE: "debug",
        XTDB_LOGGING_LEVEL_SQL: "debug",
      })
      .withCommand(["playground"])
      .withExposedPorts(5432)
      .withStartupTimeout(setupTimeout);

    if (!runningInCI) {
      containerSetup = containerSetup.withReuse();
    }

    container = await containerSetup.start();

    process.env.XTDB_HOST = "localhost";
    process.env.XTDB_PG_PORT = container.getMappedPort(5432).toString();
  }
}

export async function teardown() {
  if (container) {
    if (runningInCI) {
      await container.stop();
    } else {
      console.info(
        "Skipped teardown of XTDB playground container for reuse across tests",
      );
    }
  }
}

export function useNewDatabase() {
  process.env.XTDB_PG_DATABASE = randomBytes(16).toString("hex");
}
