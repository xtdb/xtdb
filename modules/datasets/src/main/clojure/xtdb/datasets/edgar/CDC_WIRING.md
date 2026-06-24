# EDGAR Postgres → XTDB CDC wiring

How to run the "XTDB follows Postgres" path for the EDGAR fundamentals dataset end-to-end, via the `postgres-source` SPI and `EdgarPgIndexer`.
It mirrors the GLEIF demo (`modules/datasets/demo/README.md`), adapted to the EDGAR database, tables, publication, and slot.

The shape is the same as GLEIF:

1. EDGAR lands in **Postgres** as a current-state fundamentals SOR (latest-`filed`-wins UPSERT, no history) — `xtdb.datasets.edgar.pg/submit-edgar-pg!`.
2. An **ingest node** follows that Postgres via the Postgres source + `EdgarPgIndexer`, deriving `_id` and bitemporal `_valid_from` from each row — so the XTDB side accrues the restatement history the SOR never held.
3. A **query node** attaches that database as a read-only secondary and answers the bitemporal demo queries (`xtdb.datasets.edgar/q-*`).

`EdgarPgIndexer` rides in an `IngestNode` configured **in code** (it's a plain `PgIndexer.Factory` with no `Registration`, so it can't travel as serialised secondary-database config).
That means the ingest node runs from a full-classpath JVM — a REPL on the host — not from any shipped image.

Identifiers used throughout (adapt to taste, but keep them consistent across all three steps):

| Thing | Value |
| --- | --- |
| Postgres database | `edgar` |
| Publication | `edgar_pub` |
| Replication slot | `edgar_slot` |
| Tables | `issuer`, `income_statement`, `balance_sheet` |
| Kafka log topic | `xtdb.edgar` |
| Shared storage dir | `/tmp/xt26-demo/edgar` |
| Remote alias | `pg` |
| Kafka cluster alias | `kafka` |

## 1. Enable logical replication on the `edgar` Postgres database

The Postgres source consumes logical-replication CDC, so the server must run with `wal_level=logical`.

If you're bringing Postgres up via Docker (as the GLEIF demo does), start it with the flag and point it at an init dir:

```sh
docker run -d --name edgar-pg \
  -p 127.0.0.1:5432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=edgar \
  postgres:16 \
  postgres -c wal_level=logical
```

For an already-running server, set `wal_level=logical` in `postgresql.conf` and restart (it can't be changed at runtime).
Verify:

```sql
SHOW wal_level;   -- must report 'logical'
```

## 2. Create the EDGAR tables

The tables must exist before the publication and slot can attach to them.
`xtdb.datasets.edgar.pg/create-tables!` creates all three idempotently (it also runs as part of `submit-edgar-pg!`), so the simplest path is to load some data first (step 4) — or create them up front from the REPL:

```clojure
(require '[xtdb.datasets.edgar.pg :as edgar-pg]
         '[next.jdbc :as jdbc])

(def pg (jdbc/get-connection {:dbtype "postgresql" :dbname "edgar"
                              :host "localhost" :port 5432
                              :user "postgres" :password "postgres"}))

(edgar-pg/create-tables! pg)
```

This yields:

- `issuer (cik PRIMARY KEY, entity_name, filed)`
- `income_statement (cik, period_start, period_end, <line items…>, accession, form, filed, PRIMARY KEY (cik, period_start, period_end))`
- `balance_sheet (cik, period_end, <line items…>, accession, form, filed, PRIMARY KEY (cik, period_end))`

## 3. Create the publication and confirm replica identity

Create a publication covering the EDGAR database, then leave the replication slot for the source to create on first connect.

```sql
-- Covers all three tables (and anything else in the db). If you'd rather scope
-- it, use: CREATE PUBLICATION edgar_pub FOR TABLE issuer, income_statement, balance_sheet;
CREATE PUBLICATION edgar_pub FOR ALL TABLES;
```

Don't create the slot by hand: `PostgresSource` creates `edgar_slot` itself when the ingest node opens its snapshot (it exports the snapshot from the slot's consistent point, then streams from there).
If you re-run the demo and a stale slot lingers, drop it: `SELECT pg_drop_replication_slot('edgar_slot');`.

### Replica identity — the EDGAR-specific gotcha

`EdgarPgIndexer` derives `_id` on **every** op, including `DELETE`.
A delete's `op.row` carries only the table's **REPLICA IDENTITY** columns.
Each EDGAR table's `deriveId` reads exactly its primary key:

- `issuer` → `cik` (the PK)
- `income_statement` → `cik`, `period_start`, `period_end` (the compound PK)
- `balance_sheet` → `cik`, `period_end` (the compound PK)

Postgres' **default** replica identity is the primary key, and every EDGAR table has one — so the default is already sufficient for deletes, even with the compound PKs.
You do **not** need `REPLICA IDENTITY FULL`.

There's one case where you would: if the SOR ever issues an `UPDATE` that leaves a TOAST-able column (a large `numeric`/`text`) unchanged, pgoutput sends it as "unchanged" and the source throws (`Received unchanged TOASTed column …`).
The EDGAR sink only ever UPSERTs whole rows, so this shouldn't arise; if you hand-edit rows and hit it, set `ALTER TABLE <t> REPLICA IDENTITY FULL`.

## 4. Load EDGAR into Postgres

From the REPL (see `xtdb.datasets.edgar.pg`'s `comment` block for the file-path shape):

```clojure
(require '[clojure.java.io :as io])

(edgar-pg/submit-edgar-pg!
  pg
  {:files [(io/file "data/edgar/CIK0000320193.json.gz")]})   ; Apple; add more CIKs as desired
```

Postgres now holds current-state `issuer` / `income_statement` / `balance_sheet`, keyed by their PKs, latest-`filed`-wins.

## 5. Run the ingest node (Postgres → XTDB)

The ingest node is Kotlin; drive it from the Clojure REPL via interop.
It ingests into an `edgar` database whose log is a Kafka topic and whose storage is a **local directory the query node will also read**.

```clojure
(import '[xtdb.api IngestNode$Config]
        '[xtdb.database Database$Config]
        '[xtdb.api.storage Storage]
        '[xtdb.postgres PostgresRemote$Factory PostgresSource$Factory]
        '[xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory]
        '[xtdb.datasets.edgar EdgarPgIndexer$Factory]
        '[java.nio.file Path])

;; Database.Config's positional args are (log, storage, mode, externalSource, …),
;; so build it with the copy-helpers rather than trusting arg order.
(def edgar-db
  (-> (Database$Config.)
      (.log (KafkaCluster$LogFactory. "kafka" "xtdb.edgar"))
      (.storage (Storage/local (Path/of "/tmp/xt26-demo/edgar" (make-array String 0))))
      ;; PostgresSource$Factory(remote-alias, slot-name, publication-name, indexer)
      (.externalSource (PostgresSource$Factory. "pg" "edgar_slot" "edgar_pub"
                                                (EdgarPgIndexer$Factory.)))))

(def ingest
  (-> (IngestNode$Config.)
      ;; PostgresRemote$Factory(hostname, port, database, username, password)
      (.remote "pg" (PostgresRemote$Factory. "localhost" 5432 "edgar" "postgres" "postgres"))
      (.logCluster "kafka" (KafkaCluster$ClusterFactory. "localhost:9092"))
      (.database "edgar" edgar-db)
      (.open)))
```

The node creates `edgar_slot`, snapshots the three tables, then streams CDC.
As rows arrive, `EdgarPgIndexer` derives:

- `_id` matching the native loader exactly — `cik` (issuer); `cik__income_statement__{period_start}__{period_end}` (income_statement); `cik__balance_sheet__{period_end}` (balance_sheet);
- `_valid_from` — `filed` for `issuer` / `income_statement` (a correction lives on the system axis), `period_end` for `balance_sheet` (an instant balance is as-of that date, a real valid-time timeline);
- `_valid_to` — end of time (`Long.MAX_VALUE`).

> The shape (remote → logCluster → database(log, storage, externalSource = Postgres source with `EdgarPgIndexer`) → open) is the contract.
> If a ctor signature has drifted, check `Database.Config`, `KafkaCluster.LogFactory`, `PostgresRemote.Factory`, `PostgresSource.Factory` and adjust.

## 6. Start the query node and attach `edgar`

Start a node serving pgwire, reading the same Kafka log + storage dir the ingest node writes:

```clojure
(require '[xtdb.node :as xtn]
         '[xtdb.api :as xt])

(def query-node
  (xtn/start-node
    {:server {:port 5432 :host "172.17.0.1"}            ; docker-bridge so containers reach it; not "*"
     :healthz {:port 8081 :host "172.17.0.1"}
     :log-clusters {:kafka [:kafka {:bootstrap-servers "localhost:9092"}]}
     :log [:local {:path "/tmp/xt26-demo/primary-log"}]
     :storage [:local {:path "/tmp/xt26-demo/primary-storage"}]}))

(xt/execute-tx query-node
  ["ATTACH DATABASE edgar WITH $$
      log: !Kafka
        cluster: 'kafka'
        topic: 'xtdb.edgar'
      storage: !Local
        path: '/tmp/xt26-demo/edgar'
      mode: 'read-only'
    $$"])
```

`read-only` means the query node indexes locally to serve queries but lets the ingest node own writes.
The two nodes share the `edgar` log topic and the local storage dir.

## 7. Run the demo queries

```clojure
(require '[xtdb.datasets.edgar :as edgar])

(def cik "0000320193")   ; Apple — note the 10-digit zero-padded form the loader keys on

;; restatement on the system axis: same income figure queried as-of two system-times
(xt/q query-node [edgar/q-income-as-of-system-time #inst "2023-01-01" cik #_period-end …])

;; instant balance on the valid axis
(xt/q query-node [edgar/q-balance-as-of-valid-time …])
```

Check `xtdb.datasets.edgar` for the exact query vars and their parameter order.
Queries target the `edgar` database; qualify table names (`edgar.income_statement`) if you're connected to the primary.

## Notes on identity correctness

`EdgarPgIndexer` derives `_id` from the **PG column values** (`cik`, `period_start`, `period_end`), which the sink stores in the same form the native loader uses:

- `cik` is the 10-digit zero-padded string (`parse/cik->padded`), stored verbatim in the `cik text` column.
- Dates render via `LocalDate.toString()` (canonical ISO-8601) on both sides, so `2023-09-30` is identical whether produced by the native loader or derived from the PG `date` column.

A native bulk load and a CDC stream therefore produce **identical `_id`s** for the same fact — the federation demo's whole point.

The CDC docs carry the **PG column names** (snake_case: `period_start`, `net_income_loss`) rather than the native loader's kebab keywords (`:period-start`, `:net-income-loss`).
XTDB normalises separators in SQL, so the demo queries (which use snake_case identifiers) resolve against either form — the difference is invisible at the query layer.
