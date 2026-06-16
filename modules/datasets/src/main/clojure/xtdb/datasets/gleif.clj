(ns xtdb.datasets.gleif
  "Shared load-side helpers for the curated GLEIF LEI dataset, plus the demo
   queries.

   The dataset on S3 is the idiomatic-record transit produced by
   xtdb.datasets.gleif.mirror (curation already applied) — reading it needs
   nothing from the mirror: it's plain transit + the standard serde handlers.
   `dataset` builds the {:anchor :deltas} arg both sinks take; `read-records`
   reads one mirrored file.

   The Postgres sink (xtdb.datasets.gleif.pg) loads the data; XTDB then follows
   Postgres via the GleifPgIndexer over CDC (see the `xt26` dev namespace). There
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
