(ns xtdb.datasets.edgar
  "Loads a curated slice of SEC EDGAR fundamentals into XTDB with a real
   bitemporal history.

   The dataset on S3 is the curated observation transit produced by
   xtdb.datasets.edgar.mirror (one <quarter>.transit.json.gz per quarter, the
   registry filter already applied) — but reading it needs nothing from the
   mirror: it's plain transit + the standard serde handlers, pivoted into docs by
   xtdb.datasets.edgar.parse. pg.clj reuses that same read + pivot.

   Facts are projected into wide statement tables by temporality:
   `income_statement` (duration flows), `balance_sheet` (instant balances), plus
   a static `issuer` reference.

   System-time replay (see `filing-batches`): a filing (accession) is atomic —
   all its rows commit in one transaction, never split. Filings are sorted and
   partitioned by `filed`, so whole filings sharing a date coalesce into one
   transaction stamped with that date as system-time, submitted in ascending
   order. System-time is monotonic non-decreasing in XTDB, so forward replay
   reproduces the calendar-accurate vintage history a node would have built had
   it consumed each filing as it landed — prior, since-restated values included,
   recoverable via FOR SYSTEM_TIME AS OF. Quarters are replayed oldest-first so
   system-time stays non-decreasing across the boundary.

   valid-time differs by statement: duration figures are fixed for their period
   (valid-from = filed, corrections live on the system axis); instant balances
   are as-of a date (valid-from = period end, a real valid-time timeline)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.serde :as serde])
  (:import [java.time LocalDate ZoneOffset]
           [java.util.zip GZIPInputStream]))

(defn read-records
  "Curated observation maps from a mirrored transit.json.gz — plain transit + the
   standard serde handlers, no EDGAR-specific code. Eager: the caller reads inside
   a with-open, so no lazy seq may outlive the stream."
  [^java.io.InputStream in]
  (vec (serde/read-transit-seq in :json)))

(defn- doc->row
  "A doc → the row map for put-docs, carrying its own valid-time as :xt/valid-from
   on the document itself. Per-doc (not the put-docs-level option) because each
   row has a distinct valid-from — instant balances are as-of their own date — so
   a shared option-level valid-from would be wrong."
  [doc]
  (let [vf (:valid-from doc)
        vt (:valid-to doc)]
    (cond-> (dissoc doc :table :valid-from :valid-to)
      vf (assoc :xt/valid-from vf)
      vt (assoc :xt/valid-to vt))))

(defn filing-batches
  "Order docs into per-filing-date transaction batches. Docs are grouped by
   :accession (a filing is atomic — never split across transactions), the filing
   groups sorted and partitioned by :filed, so each batch holds whole filings
   that share a filing date. Returns an ascending seq of [^LocalDate filed docs].

   Partition-by is safe here: it runs over filing groups we've materialised and
   sorted ourselves, not the unordered source rows."
  [docs]
  (->> (vals (group-by :accession docs))      ; whole filings, never split
       (sort-by #(:filed (first %)))          ; ascending filing date
       (partition-by #(:filed (first %)))     ; same-date filings coalesce
       (map (fn [filing-groups]
              [(:filed (ffirst filing-groups)) (apply concat filing-groups)]))))

(defn- submit-filing-batch!
  "Submit one date-batch as a transaction at that date's system-time. One put-docs
   per table (a put-docs targets a single table); rows carry their own
   :xt/valid-from."
  [node ^LocalDate filed docs]
  (let [opts {:system-time (.toInstant (.atStartOfDay filed ZoneOffset/UTC))}
        tx-ops (->> (group-by #(:table (meta %)) docs)
                    (map (fn [[table table-docs]]
                           (into [:put-docs {:into table}] (map doc->row table-docs)))))]
    (xt/submit-tx node (vec tx-ops) opts)))

(defn submit-docs!
  "Submit a seq of statement/issuer docs as a bitemporal basis: one transaction
   per filing date (see `filing-batches`), stamped at that date's system-time, in
   ascending order — so each filing's view is recorded as of when it was filed
   and a later restatement supersedes it in system-time without overwriting the
   prior basis. Returns the last tx-key."
  [node docs]
  (reduce (fn [_ [filed batch-docs]] (submit-filing-batch! node filed batch-docs))
          nil
          (filing-batches docs)))

(defn- transit-files
  "The mirrored per-quarter transit files under `dir`, chronological. Quarters
   sort lexically (2025q2 < 2025q3 < … < 2026q1), so a name sort is publish order."
  [^java.io.File dir]
  (for [^java.io.File f (sort (.listFiles dir))
        :when (str/ends-with? (.getName f) ".transit.json.gz")]
    f))

(defn dataset
  "The mirrored EDGAR dataset under `data-dir`: a {:quarters [file ...]} map, the
   per-quarter transit files in chronological (oldest-first) order."
  [data-dir]
  {:quarters (vec (transit-files (io/file data-dir)))})

(defn submit-quarter!
  "Replays one quarter's mirrored transit file into `node`: read the curated
   observations, pivot to docs, submit per-filing-date (see `submit-docs!`).
   Only this quarter is held in memory."
  [node file]
  (with-open [in (-> (io/input-stream file) GZIPInputStream.)]
    ;; read-records realises eagerly — the transit seq is backed by `in`, which
    ;; closes when this scope exits, so no lazy seq may outlive it.
    (let [docs (parse/observations->docs (read-records in))]
      (log/infof "  %s — %d docs" (.getName (io/file file)) (count docs))
      (submit-docs! node docs))))

(defn submit-edgar!
  "Replays the mirrored EDGAR dataset into `node`, one quarter at a time so only a
   single quarter is held in memory. Quarters replay oldest-first (the `dataset`
   order) so system-time stays non-decreasing across the boundary."
  [node {:keys [quarters]}]
  (log/info "Loading" (count quarters) "EDGAR quarter(s)...")
  (reduce (fn [_ file]
            (when (Thread/interrupted)
              (throw (InterruptedException. "interrupted loading EDGAR quarters")))
            (submit-quarter! node file))
          nil
          quarters)
  (log/info "EDGAR load complete."))

;;; Demo queries — run via (xt/q node [<query> & params]).

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-income-as-of-system-time
  "An income-statement figure as XTDB knew it at a given system-time. The
   statement is one entity per (cik, period), so a restatement supersedes the
   prior values in system-time — FOR SYSTEM_TIME AS OF returns the single belief
   current at T. Run before vs after a restatement's filing date to see the
   value change; the earlier instant still yields the original, since prior
   system-time is immutable."
  ;; params: [system-time-instant, cik, period-end-date]
  "SELECT s.net_income_loss, s.form, s.filed
   FROM income_statement FOR SYSTEM_TIME AS OF ? AS s
   WHERE s.cik = ? AND s.period_end = ?")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-income-restatement-history
  "Every vintage of an income statement for one period, oldest filing first — the
   restatement trail. FOR ALL SYSTEM_TIME yields each belief we held; where a
   value changes between consecutive `filed` dates, a restatement occurred."
  ;; params: [cik, period-end-date]
  "SELECT s.net_income_loss, s.form, s.filed, s.accession
   FROM income_statement FOR ALL SYSTEM_TIME AS s
   WHERE s.cik = ? AND s.period_end = ?
   ORDER BY s.filed")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-balance-as-of-valid-time
  "The assets balance effective at a real-world date — the most recent as-of
   balance valid then. Instant balances carry valid-from = their period end, so
   FOR VALID_TIME AS OF walks the genuine valid-time timeline; ordering by
   period_end desc and taking the first gives the balance in force on that date."
  ;; params: [valid-date, cik]
  "SELECT b.assets, b.period_end
   FROM balance_sheet FOR VALID_TIME AS OF ? AS b
   WHERE b.cik = ? AND b.assets IS NOT NULL
   ORDER BY b.period_end DESC
   LIMIT 1")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-issuer
  "The issuer reference for a cik, as XTDB knows it now."
  ;; params: [cik]
  "SELECT i.cik, i.entity_name FROM issuer AS i WHERE i.cik = ?")

(comment
  ;; End-to-end against an in-memory node. Get the dataset first: sync it down
  ;; (scripts/download-dataset.sh --edgar) or produce it locally
  ;; (xtdb.datasets.edgar.mirror/mirror!) — both leave transit under the dir below.
  (require '[xtdb.node :as xtn] :reload)

  (def ds (dataset "src/dev/resources/data/edgar"))

  (def node (xtn/start-node {}))

  (submit-edgar! node ds)

  (xt/q node ["SELECT COUNT(*) AS c FROM income_statement"])

  (.close node))
