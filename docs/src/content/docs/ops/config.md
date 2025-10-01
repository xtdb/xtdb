---
title: Configuration
---

<details>
<summary>Changelog (last updated v2.1)</summary>

v2.1: multi-database support

: The log and storage configurations were changed as part of 2.1's multi-db support. 

  For more details on those changes, see the [Transaction Logs](config/log) and [Object Storage](config/storage) documentation.
    
</details>

XTDB nodes are configured using YAML files.

The two main pluggable components of XTDB are transaction logs and object storage - these can be configured in the `databases` section of the configuration file.

``` yaml
## -- optional

## transaction log configuration
## defaults to an in-memory transaction log
log: !Local
  path: /path/to/log-file

## object store configuration
## defaults to an in-memory object store
storage: !Local
  path: /path/to/storage-dir
```

If no `databases` section is specified, XTDB will use the default configuration - an in-memory transaction log and an in-memory object store.

For more details on the log and storage configurations, including the available components, see the [Transaction Logs](config/log) and [Object Storage](config/storage) documentation.

## Using `!Env`

For certain keys, we allow the use of environment variables - typically, the keys where we allow this are things that may change **location** across environments.
Generally, they are either "paths" or "strings".

When specifying a key, you can use the `!
Env` tag to reference an environment variable.
As an example:

``` yaml
storage: !Local
  path: !Env XTDB_STORAGE_PATH
```

Any key that we allow the use of `!
Env` will be documented as such.

## Monitoring & Observability

XTDB provides a suite of tools & templates to facilitate monitoring and observability.
See [Monitoring & Observability](config/monitoring).

## Authentication

The pg-wire server supports authentication which can be configured via authentication rules.
See [Authentication](config/authentication).

## Other configuration:

XTDB nodes accept other optional configuration, as follows:

``` yaml
server:
  # Host on which to start a read-write Postgres wire-compatible server.
  #
  # Default is "localhost", which means the server will only accept connections on the loopback interface.
  # Set to '*' to accept connections on all interfaces.
  host: localhost

  # Port on which to start a read-write Postgres wire-compatible server.
  #
  # Default is 0, to have the server choose an available port.
  # (In the XTDB Docker images, this is defaulted to 5432.)
  # Set to -1 to not start a read-write server.
  port: 0

  # Port on which to start a read-only Postgres wire-compatible server.
  #
  # The server on this port will reject any attempted DML/DDL,
  # regardless of whether the user would otherwise have the permission to do so.
  #
  # Default is -1, to not start a read-only server.
  # Set to 0 to have the server choose an available port.
  readOnlyPort: -1

compactor:
  # Number of threads to use for compaction.

  # Defaults to min(availableProcessors / 2, 1).
  # Set to 0 to disable the compactor.
  threads: 4
```

## CLI tools/flags

<details>
<summary>Changelog (last updated v2.1)</summary>

v2.1: top-level commands

: In v2.1, we changed the CLI to use top-level commands (not dissimilar to Git, for example).

  Previously, the playground and compact-only nodes were activated using optional flags - `--playground-port` and `--compact-only` respectively.

  `reset-compactor` was also added in v2.1.
    
</details>

You can run various tools by passing arguments - either directly to the CLI or via Docker's arguments:

`node` (default, can be omitted)
: - `-f <file>`, `--file <file>`: specifies the configuration file
    to use.

`playground`
: Starts a playground - an in-memory server that will accept any
    database name, creating it if required.

    - `-p <port>`, `--port <port>` (default 5432): specifies the port to run the playground server on.

`compactor`
: Starts a compactor-only node - useful for giving the compaction
    process more compute resources.

    - `-f <file>`, `--file <file>`: specifies the configuration file to use.

`reset-compactor`
: Resets the compaction back to L0, deleting any L1+ files - use this
    if you've encountered a compaction bug and need to reset its state.

    1. Spin down all of your XT nodes
    2. Using your container orchestration tool (e.g.
      Kubernetes), run a one-shot task with an overriden command: `["reset-compactor"]`.
    Optionally, specify `--dry-run` to list all of the files to be removed.

    3. When the tool has finished, spin up your nodes again.

    You may want to also spin up a compactor-only node to help out
    with the re-compaction.

    At the moment, this can only reset all the way back to L0 -
    finer-grained reset will be added in a later release.

`read-arrow-file <file>`
: reads an Arrow file and emits it as EDN

`read-arrow-stream-file <file>`
: reads an Arrow 'stream IPC format' file and emits it as EDN

e.g.

- Dockerfile: `CMD ["playground", "--port", "5439"]`
- docker-compose: `command: ["playground", "--port", "5439"]`
- Java uberjar: `java -jar xtdb.jar playground --port 5439`
- Clojure (with `xtdb-core` in your `deps.edn`): `clj -M xtdb.main playground --port 5439`

You can also pass `--help` to any of the commands to get command-specific help.
