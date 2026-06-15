(ns xtdb.datasets.edgar
  "Loads SEC EDGAR fundamentals into XTDB with a real bitemporal history.

   Two front-ends produce the same docs (xtdb.datasets.edgar.parse / .tsv):
   - tsv: the quarterly 'Financial Statement Data Sets' — every filer, cut by
     time. This is the demo's breadth/streaming source (`submit-quarters!`).
   - parse: companyfacts JSON — one company, full history. The per-company
     alternative (`submit-edgar!`), curated by `demo-cik-allow-set`.

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
   recoverable via FOR SYSTEM_TIME AS OF.

   valid-time differs by statement: duration figures are fixed for their period
   (valid-from = filed, corrections live on the system axis); instant balances
   are as-of a date (valid-from = period end, a real valid-time timeline)."
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.datasets.edgar.tsv :as tsv])
  (:import [java.io Reader]
           [java.time LocalDate ZoneOffset]))

(def demo-cik-allow-set parse/demo-cik-allow-set)

(defn ->reader
  "A Reader is returned as-is; anything else is opened via parse/gz-reader. Lets
   the sink entry points accept either a ready reader or a (gzipped) file."
  ^Reader [f]
  (if (instance? Reader f) f (parse/gz-reader f)))

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

(defn submit-companyfacts!
  "Submits one companyfacts document as a bitemporal basis."
  [node {:keys [reader allow-set]
         :or {allow-set demo-cik-allow-set}}]
  (with-open [rdr reader]
    ;; read-docs realises eagerly — the JSON seq is backed by `rdr`, which closes
    ;; when this scope exits, so no lazy seq may outlive it.
    (submit-docs! node (parse/read-docs rdr allow-set))))

(defn submit-edgar!
  "Replays a collection of companyfacts files into `node`. Each file is one
   issuer's full fact history; the per-filing system-time grouping inside
   `submit-companyfacts!` does the temporal ordering, so file order between
   issuers doesn't matter.

   files: seq of readers or anything `parse/gz-reader`-able."
  [node {:keys [files allow-set] :or {allow-set demo-cik-allow-set}}]
  (log/info "Loading" (count files) "EDGAR companyfacts file(s)...")
  (reduce (fn [_ f]
            (submit-companyfacts! node {:reader (->reader f) :allow-set allow-set}))
          nil
          files))

(defn submit-quarters!
  "Replays EDGAR quarterly TSV dumps into `node`, one quarter at a time so only a
   single quarter is held in memory. Each quarter is a [sub-file num-file] pair
   (readers or gz-reader-able). Within a quarter, filings replay in filing-date
   order; quarters should be supplied oldest-first so system-time stays
   non-decreasing across the boundary."
  [node quarters]
  (reduce (fn [_ [sub-f num-f]]
            (with-open [sub-rdr (->reader sub-f)
                        num-rdr (->reader num-f)]
              (submit-docs! node (tsv/quarter->docs sub-rdr num-rdr))))
          nil
          quarters))

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
