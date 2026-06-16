(ns xtdb.datasets.gleif
  "Shared load-side helpers for the curated GLEIF LEI dataset, plus the demo
   queries.

   The dataset on S3 is the idiomatic-record transit produced by
   xtdb.datasets.gleif.mirror (curation already applied) — reading it needs
   nothing from the mirror: it's plain transit + the standard serde handlers.
   `dataset` builds the {:anchor :deltas} arg both sinks take; `read-records`
   reads one mirrored file.

   The Postgres sink (xtdb.datasets.gleif.pg) loads the data; XTDB then follows
   Postgres via the GleifPgIndexer over CDC (end-to-end run-through in the comment
   at the bottom of this ns, and in modules/datasets/gleif/README.md). There
   is no native XTDB loader — the federation *is* the XTDB-population story."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.transit :as transit]
            [xtdb.serde :as serde])
  (:import [java.time Instant LocalDateTime ZoneOffset]
           [java.time.format DateTimeFormatter]))

(defn read-records
  "Idiomatic GLEIF records from a mirrored transit.json.gz — plain transit + the
   standard serde handlers, no GLEIF-specific code. Eager: the caller reads inside
   a with-open, so no lazy seq may outlive the stream."
  [^java.io.InputStream in]
  (vec (serde/transit-seq (transit/reader in :json {:handlers serde/transit-read-handler-map}))))

(def ^:private stamp-fmt (DateTimeFormatter/ofPattern "yyyyMMdd_HHmmss"))

(defn- file->date ^Instant [^java.io.File f]
  (-> (LocalDateTime/parse (second (re-find #"(\d{8}_\d{6})" (.getName f))) stamp-fmt)
      (.toInstant ZoneOffset/UTC)))

(defn- transit-files [^java.io.File dir]
  (for [^java.io.File f (sort (.listFiles dir))
        :when (str/ends-with? (.getName f) ".transit.json.gz")]
    {:date (file->date f) :file f}))

(defn dataset [data-dir]
  (let [dir (io/file data-dir)]
    {:anchor (first (transit-files (io/file dir "anchor")))
     :deltas (transit-files (io/file dir "deltas"))}))

;;; Demo queries — run via (xt/q node [<query> & params]) against the `gleif`
;;; database the GleifPgIndexer populates over CDC.

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-entity-as-of-system-time
  "Entity state as XTDB knew it at a given system-time. Run at T1 (before a
   correction) and again at a later T2 (after it landed): the same query at T1
   still returns the original, proving the prior basis is immutable."
  ;; params: [system-time-instant, lei]
  "SELECT r._id, r.legal_name, r.entity_status, r._valid_from, r._valid_to
   FROM lei FOR SYSTEM_TIME AS OF ? AS r
   WHERE r._id = ?")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-entity-as-of-valid-time
  "Corrected truth: the value effective at a real-world date, as XTDB knows it
   now — even if the correction was recorded (system-time) later."
  ;; params: [valid-date, lei]
  "SELECT r._id, r.legal_name, r.entity_status, r._valid_from, r._valid_to
   FROM lei FOR VALID_TIME AS OF ? AS r
   WHERE r._id = ?")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-legal-name-history
  "An entity's legal-name history as bitemporal state: every version the name has
   held, with the valid-time span each was effective. The state-change events
   wrote one version per change (valid-from the effective date), so XT's
   _valid_from/_valid_to bracket the real-world intervals."
  ;; params: [lei]
  "SELECT n.legal_name, n._valid_from, n._valid_to
   FROM lei_legal_name_changes FOR ALL VALID_TIME AS n
   WHERE n._id = ?
   ORDER BY n._valid_from")

(comment
  ;; ── GLEIF federation demo — end-to-end run-through ───────────────────────────
  ;;
  ;; Postgres is the current-state sec-master; XTDB follows it over CDC via the
  ;; GleifPgIndexer, accruing the bitemporal history Postgres never held. Run from
  ;; the root REPL — :xtdb-datasets is on its classpath, so `!Gleif` resolves on
  ;; the node. See modules/datasets/gleif/README.md for the prose walkthrough.
  ;;
  ;; NOTE: the node + ATTACH config (steps 2–3) is reconstructed from the tests —
  ;; the original driver ns wasn't committed — so sanity-check it before relying on
  ;; it verbatim. The Postgres load itself (step 4) is verified as-is.

  ;; 1. Supporting cast (Kafka, Postgres, Metabase, Prometheus, Grafana):
  ;;      cd modules/datasets/gleif && docker-compose up -d
  ;;    Get the dataset first — scripts/download-dataset.sh --gleif (S3, needs AWS creds) 
  ;;    or produce it locally via xtdb.datasets.gleif.mirror/mirror!.

  ;; 2. Start a queryable node bound to the compose network gateway (172.30.0.1, NOT '*' —
  ;;    reachable by the containers, but not by an untrusted LAN). The `pg` remote
  ;;    is what the attached gleif db's !Postgres source consumes; the :kafka
  ;;    log-cluster is what its !Kafka log (step 3) references by name.

  (require '[xtdb.node :as xtn] '[xtdb.api :as xt] '[next.jdbc :as jdbc])

  (def node
    (xtn/start-node
      {:server {:host "172.30.0.1" :port 5432}
       :healthz {:host "172.30.0.1" :port 8081}     ; Prometheus scrapes 172.30.0.1:8081
       :log-clusters {:kafka [:kafka {:bootstrap-servers "localhost:9092"}]}
       :log [:kafka {:cluster :kafka :topic "xtdb.log"}]
       :storage [:local {:path "/tmp/gleif-demo/xtdb"}]
       :remotes {:pg [:postgres {:host "localhost" :database "gleif"
                                 :username "postgres" :password "postgres"}]}}))

  ;; 3. ATTACH the gleif database while Postgres is still empty — the !Postgres
  ;;    source snapshots then tails CDC through the !Gleif indexer, deriving _id and
  ;;    valid-time per table family (see GleifPgIndexer). Attaching first means the
  ;;    step-4 load streams in over CDC rather than a silent bulk snapshot. ATTACH
  ;;    must run against the primary `xtdb` database.
  
  (with-open [conn (.build (.createConnectionBuilder node))]
    (jdbc/execute! conn ["ATTACH DATABASE gleif WITH $$
                            log: !Kafka { cluster: 'kafka', topic: 'xtdb.gleif' }
                            storage: !Local { path: '/tmp/gleif-demo/gleif' }
                            externalSource: !Postgres
                              remote: 'pg'
                              slotName: 'gleif_slot'
                              publicationName: 'gleif_pub'
                              indexer: !Gleif {}
                          $$"]))

  ;; 4. Load GLEIF into Postgres and watch it stream into XTDB over CDC — poll
  ;;    `SELECT count(*) FROM gleif.lei` on the node and see it climb to ~228k.
  ;;    (psql checks + progress logging live in the xtdb.datasets.gleif.pg comment.)

  (require '[xtdb.datasets.gleif.pg :as gleif-pg])

  (with-open [pg-conn (jdbc/get-connection 
                        {:dbtype "postgresql" :dbname "gleif"
                         :host "localhost" :port 5432
                         :user "postgres" :password "postgres"})]
    (gleif-pg/submit-gleif-pg! pg-conn (dataset "src/test/resources/data/gleif"))) ; ~227,922 lei rows

  ;; 5. Query the bitemporal history against the `gleif` database. q-legal-name-history
  ;;    et al. (above) show truth-as-of-then vs corrected-truth-now. Pick a real LEI
  ;;    off the loaded data first: (jdbc/execute! pg-conn ["SELECT lei FROM lei LIMIT 5"]).
  
  (with-open [conn (.build (-> (.createConnectionBuilder node) (.database "gleif")))]
    (xt/q conn [q-legal-name-history "<lei>"]))

  (.close node))
