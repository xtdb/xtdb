---
title: Installation via Docker
---


## Try Online

If you want to avoid running your own XTDB server locally, you can instantly play with inserting data and querying right now using the [XTDB Play](https://play.xtdb.com/) web-based console.
The interactive [SQL Quickstart](/quickstart/sql-overview.html) uses this console to showcase XTDB's SQL dialect and bitemporal capabilities.

Otherwise, let's get XTDB downloaded and running on your own machine...​

## Docker Install

XTDB supports production usage via the Postgres wire protocol so that you can work with various Postgres-compatible tools, drivers etc.

You can start a 'standalone' (i.e. non-production, non-distributed) XTDB server using the following command:

``` bash
## see https://github.com/xtdb/xtdb/pkgs/container/xtdb/versions for tags
## `latest`: latest tagged release
## `nightly`: built every night from `main` branch
## `edge`: latest nightly plus urgent fixes

docker run -it --pull=always \
  -p 5432:5432 \
  -p 8080:8080 \
  ghcr.io/xtdb/xtdb

## 5432: Postgres wire-compatible server (primary API)
## 8080: Monitoring/healthz HTTP endpoints
```

This command starts a Postgres wire-compatible endpoint on port 5432.

By default your data will only be stored temporarily using a local directory within the Docker container.

### (Optional) Mount a host directory

You can attach a host volume to preserve your data across container restarts, e.g. by adding `-v /tmp/xtdb-data-dir:/var/lib/xtdb`, however because the XTDB container runs as a non-root user by default (UID 20000), you must ensure the container can write to it:

- For Docker, before running the container:
  
  ``` bash
  sudo chown -R 20000:20000 /tmp/xtdb-data-dir
  ```

- For Podman, ensure the directory is owned by your user and use `--userns=keep-id`, e.g.:

  ``` bash
  podman run -it --pull=always \
    --userns=keep-id \
    -p 5432:5432 \
    -p 8080:8080 \
    -v /tmp/xtdb-data-dir:/var/lib/xtdb \
    ghcr.io/xtdb/xtdb
  ```

## Wait for XTDB to start

After seeing a 'Node started' log message (e.g. `09:00:00 | INFO xtdb.cli | Node started`) you are able to confirm your XTDB server is running using cURL:

``` bash
curl http://localhost:8080/healthz/alive

## Alive.
```

## Connect with `psql`

``` bash
## if you have Postgres installed, psql is already available
psql -h localhost -U xtdb xtdb
```

## Run your first query

```
psql (16.2, server 16)
Type "help" for help.

user=> SELECT 'foo' AS bar;

 bar
-----
 foo
(1 row)
```

Next up:

- if you haven't already run through the Quickstart already, you probably want to start there with inserting data, running your first queries, and learning about XTDB's novel capabilities: [let's INSERT some data](/quickstart/sql-overview)!
- to connect to XTDB from your language/tool of choice, have a look at XTDB's [driver support](/drivers).
