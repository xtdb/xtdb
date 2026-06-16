(ns xt26
  "The XT26 GLEIF federation demo, driven from this dev REPL — 'XTDB follows
   Postgres'. GLEIF lands in Postgres as a current-state securities-master (no
   history); a full XTDB node attaches it as a secondary via the Postgres source
   + GleifPgIndexer, deriving bitemporal valid-time from the rows as they arrive
   over CDC — so XTDB accrues the history the SOR never held. The same node serves
   pgwire, so the demo queries (and Metabase) run straight against it.

   Lives in the dev source set, not the datasets module, so the whole repo is on
   the classpath — in particular xtdb.kafka, whose `:kafka` log-cluster tag the
   node config resolves, and GleifPgIndexer, a code-supplied indexer that has to
   be on the node's classpath (a REPL gives us that; no shipped image does).

   == Before the REPL ==

   Bring up the supporting cast (Kafka, Postgres, Metabase, Prometheus, Grafana):

     cd modules/datasets/xt26-demo && docker compose up -d

   Postgres runs its init/ scripts only on a *fresh* data volume — if the `gleif`
   db/schema is missing or stale from a prior run, recreate it:
   `docker compose rm -f -s -v postgres && docker compose up -d postgres`.
   (See the docker-compose.yml header for ports + the 127.0.0.1 binding rationale.)

   Get the dataset (curated GB slice, ~228k entities, 2025-06-15 anchor + a
   12-month delta tail) down from S3:

     modules/datasets/scripts/download-dataset.sh --gleif

   Then `./gradlew :clojureRepl` and work the comment block at the foot of this ns
   form-by-form. Order matters: start the node + attach `gleif` BEFORE loading
   Postgres, so you watch the rows stream in over CDC rather than find them there.

   == Surfaces ==

   - Metabase (http://localhost:3001): add a PostgreSQL database — host
     172.17.0.1, port 5432, db `gleif`, user `xtdb`, blank password (XTDB speaks
     pgwire). Build cards on the demo queries below.
   - Grafana (http://localhost:3000, admin/admin): Prometheus datasource is
     provisioned; explore XTDB metrics or import from monitoring/dev-dashboards/.

   == Teardown ==

     docker compose down [-v]      # -v also wipes Metabase/Grafana volumes
     rm -rf /tmp/xt26-demo         # the XTDB log/storage dirs"
  (:require [xtdb.api :as xt]
            [xtdb.datasets.gleif :as gleif]
            [xtdb.datasets.gleif.pg :as gleif-pg]
            [xtdb.node :as xtn]
            [next.jdbc :as jdbc]))

(def ^:private data-dir "modules/datasets/src/dev/resources/data/gleif")

(defn start-node!
  "A full node: pgwire on the docker bridge (172.17.0.1, reachable from the demo
   containers but not the venue LAN — never \"*\"), a Kafka log cluster, and the
   `pg` Postgres remote the gleif source draws on."
  []
  (xtn/start-node
   {:server {:port 5432 :host "172.17.0.1"}
    :healthz {:port 8081 :host "172.17.0.1"}                 ; Prometheus scrapes /metrics
    :log-clusters {:kafka [:kafka {:bootstrap-servers "localhost:9092"}]}
    :log [:local {:path "/tmp/xt26-demo/primary-log"}]
    :storage [:local {:path "/tmp/xt26-demo/primary-storage"}]
    ;; 127.0.0.1, not "localhost": the latter can resolve to IPv6 ::1, but docker
    ;; publishes Postgres on IPv4 127.0.0.1:5432 only.
    :remotes {:pg [:postgres {:host "127.0.0.1" :database "gleif"
                              :username "postgres" :password "postgres"}]}}))

(defn attach-gleif!
  "Attach `gleif` as a secondary: its own Kafka log + local storage, fed from the
   `pg` remote via the Postgres source with our bitemporal indexer. CDC streams
   from here — the node snapshots Postgres, then follows it live. GleifPgIndexer
   derives _id/_valid_from per table family: the `lei` entity keyed by lei (valid
   from system-time); the *_changes state transitions keyed by lei (valid from
   effective_date, so successive changes are versions of one doc — the bitemporal
   name/address history); the occurrences keyed lei__effective_date."
  [node]
  ;; ATTACH DATABASE can't run inside a transaction (it's DDL), so this goes over
  ;; a plain autocommit statement on the node's connection, not xt/execute-tx.
  (with-open [conn (.build (.createConnectionBuilder node))
              stmt (.createStatement conn)]
    (.execute stmt
              "ATTACH DATABASE gleif WITH $$
                 log: !Kafka
                   cluster: 'kafka'
                   topic: 'xtdb.gleif'
                 storage: !Local
                   path: '/tmp/xt26-demo/gleif'
                 externalSource: !Postgres
                   remote: 'pg'
                   slotName: 'gleif_slot'
                   publicationName: 'gleif_pub'
                   indexer: !Gleif {}
               $$")))

(defn pg-conn []
  (jdbc/get-connection {:dbtype "postgresql" :dbname "gleif"
                        :host "127.0.0.1" :port 5432
                        :user "postgres" :password "postgres"}))

(defn load-pg!
  "Load the GLEIF dataset into Postgres; the attached node picks it up over CDC."
  [conn]
  (gleif-pg/submit-gleif-pg! conn (gleif/dataset data-dir)))

(comment
  ;; 1. start the node + attach gleif (CDC begins, Postgres still empty):
  (def node (start-node!))
  (attach-gleif! node)

  ;; watch it arrive — re-run as the load below progresses:
  (xt/q node ["SELECT COUNT(*) c FROM gleif.lei"])

  ;; 2. NOW load Postgres — rows stream into XT over CDC:
  (def conn (pg-conn))
  (user/set-log-level! 'xtdb.datasets.gleif.pg :debug)   ; per-file/-batch progress
  (load-pg! conn)

  ;; 3. the shiny bit — pick a GB entity that genuinely renamed itself:
  ;;    984500Z4F0C6A9BE0594 went TRADEWAVE CAPITAL PLC → TRADE HEDGE PLC →
  ;;    TRADE HEDGE LIMITED. (Find your own with the FOR ALL VALID_TIME query below.)
  (def lei "984500Z4F0C6A9BE0594")

  ;; Postgres (the SOR) knows only the *current* name — its UPSERT overwrote the rest:
  (jdbc/execute! conn ["SELECT legal_name FROM lei WHERE lei = ?" lei])

  ;; XTDB has the full valid-time history — what it was called, when, each name
  ;; bracketed by the dates it was in force:
  (xt/q node [gleif/q-legal-name-history lei])

  ;; and the system-time axis: the rename to LIMITED was effective 2025-08-06 but
  ;; only recorded ~2025-09-10. Same question about a past date, two answers —
  ;; "as of when we asked". Postgres structurally can't express this.
  (xt/q node ["SELECT n.legal_name FROM gleif.lei_legal_name_changes
               FOR SYSTEM_TIME AS OF TIMESTAMP '2025-09-01Z'
               FOR VALID_TIME AS OF TIMESTAMP '2025-08-20Z' AS n WHERE n._id = ?" lei])
  (xt/q node ["SELECT n.legal_name FROM gleif.lei_legal_name_changes
               FOR VALID_TIME AS OF TIMESTAMP '2025-08-20Z' AS n WHERE n._id = ?" lei])

  ;; the prepared demo queries (params: see each def in xtdb.datasets.gleif):
  (xt/q node [gleif/q-entity-as-of-system-time #inst "2025-07-01" lei])
  (xt/q node [gleif/q-entity-as-of-valid-time #inst "2025-09-01" lei])

  ;; to find a fresh subject with the richest name history:
  (xt/q node ["SELECT n._id, COUNT(DISTINCT n.legal_name) names
               FROM gleif.lei_legal_name_changes FOR ALL VALID_TIME AS n
               GROUP BY n._id ORDER BY names DESC LIMIT 5"])

  (.close conn)
  (.close node))
