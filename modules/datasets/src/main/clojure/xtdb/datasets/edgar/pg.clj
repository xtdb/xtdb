(ns xtdb.datasets.edgar.pg
  "Loads the EDGAR fundamentals dataset into a plain Postgres database — the
   fundamentals system-of-record half of the demo, distinct from the bitemporal
   XTDB sink.

   Postgres holds current state only: each statement row UPSERTs by its period
   key, keeping the most recently filed vintage. The restatement history (the
   prior, since-corrected values) and the instant valid-time timeline live in
   XTDB, not here — a real fundamentals SOR carries the latest figure, which is
   the point the federation demo makes.

   Reads the same mirrored transit the XTDB sink does (xtdb.datasets.edgar
   read-records + parse/observations->docs) and UPSERTs the current-state rows.

   The schema (tables, publication) is owned by the demo's
   init/postgres/01-edgar.sql — this loader only UPSERTs into it. The line-item
   columns there must stay in lockstep with parse/statement-column-names."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.parse :as parse])
  (:import [java.util.zip GZIPInputStream]))

;; pgjdbc maps java.time.LocalDate ↔ date and BigDecimal ↔ numeric directly, and
;; the parse layer already yields those — so columns bind without coercion.

(defn- upsert-sql
  "Build an UPSERT keeping the latest vintage: overwrite only when the incoming
   filing is at least as recent (files arrive in arbitrary order, so guard on
   `filed` rather than trusting load order). `key-cols` are the conflict target."
  [table key-cols value-cols]
  (let [all-cols (concat key-cols value-cols)
        placeholders (str/join "," (repeat (count all-cols) "?"))
        updates (->> value-cols
                     (map #(str (name %) " = EXCLUDED." (name %)))
                     (str/join ", "))]
    (format "INSERT INTO %s (%s) VALUES (%s)
             ON CONFLICT (%s) DO UPDATE SET %s
             WHERE EXCLUDED.filed >= %s.filed"
            (name table)
            (str/join "," (map name all-cols))
            placeholders
            (str/join "," (map name key-cols))
            updates
            (name table))))

(defn- statement-key-cols [statement]
  (case statement
    :income_statement [:cik :period-start :period-end]
    :balance_sheet [:cik :period-end]))

(defn- statement-value-cols [statement]
  (concat (parse/statement-column-names statement) [:accession :form :filed]))

(defn- statement-params [statement doc]
  ;; column keys in the doc match the param order: key-cols then value-cols.
  ;; pgjdbc binds positionally, so a column with `-` (e.g. :period-start) and the
  ;; snake-cased line items both resolve by their doc key — no name translation.
  (mapv #(get doc %) (concat (statement-key-cols statement)
                             (statement-value-cols statement))))

(def ^:private issuer-upsert
  "INSERT INTO issuer (cik, entity_name, filed) VALUES (?,?,?)
   ON CONFLICT (cik) DO UPDATE SET
     entity_name = EXCLUDED.entity_name,
     filed = LEAST(issuer.filed, EXCLUDED.filed)")

(defn- upsert! [conn sql ->params docs]
  (when (seq docs)
    (jdbc/execute-batch! conn sql (mapv ->params docs) {})))

(def ^:private income-upsert
  (delay (upsert-sql :income_statement
                     (statement-key-cols :income_statement)
                     (statement-value-cols :income_statement))))

(def ^:private balance-upsert
  (delay (upsert-sql :balance_sheet
                     (statement-key-cols :balance_sheet)
                     (statement-value-cols :balance_sheet))))

(defn submit-docs!
  "UPSERT a seq of statement/issuer docs into Postgres, one transaction per
   filing — the atomic unit a real feed would commit (whole filings sharing a
   date coalesce; see `edgar/filing-batches`). Postgres keeps current state, so
   ordering only matters for the latest-vintage-wins guard on `filed`."
  [conn docs]
  (doseq [[_filed batch-docs] (edgar/filing-batches docs)]
    (let [{:keys [issuer income_statement balance_sheet]}
          (group-by #(:table (meta %)) batch-docs)]
      (jdbc/with-transaction [tx conn]
        (upsert! tx issuer-upsert
                 (fn [doc] [(:cik doc) (:entity-name doc) (:filed doc)]) issuer)
        (upsert! tx @income-upsert (partial statement-params :income_statement)
                 income_statement)
        (upsert! tx @balance-upsert (partial statement-params :balance_sheet)
                 balance_sheet)))))

(defn- submit-quarter-pg!
  "UPSERT one quarter's mirrored transit file into Postgres via `conn`. Only this
   quarter is held in memory."
  [conn file]
  (with-open [in (-> (io/input-stream file) GZIPInputStream.)]
    (let [docs (parse/observations->docs (edgar/read-records in))]
      (log/infof "  %s — %d docs" (.getName (io/file file)) (count docs))
      (submit-docs! conn docs))))

(defn submit-edgar-pg!
  "Loads the mirrored EDGAR dataset into Postgres via `conn` (a next.jdbc
   connectable), UPSERTing current state, one quarter at a time. system-time is
   irrelevant here since Postgres keeps latest only — ordering only matters for
   the latest-vintage-wins guard on `filed`.

   The schema (tables, publication) is owned by the demo's
   init/postgres/01-edgar.sql — this loader only UPSERTs into it."
  [conn {:keys [quarters]}]
  (log/info "Loading" (count quarters) "EDGAR quarter(s) into Postgres...")
  (doseq [file quarters]
    (when (Thread/interrupted)
      (throw (InterruptedException. "interrupted loading EDGAR into Postgres")))
    (submit-quarter-pg! conn file))
  (log/info "EDGAR Postgres load complete."))

(comment
  ;; End-to-end against the demo Postgres (xt26-demo/docker-compose.yml — db
  ;; `edgar`, schema owned by init/postgres/01-edgar.sql). Get the dataset first:
  ;; sync it down (scripts/download-dataset.sh --edgar) or produce it locally
  ;; (xtdb.datasets.edgar.mirror/mirror!) — both leave transit under the dir below.
  (with-open [conn (jdbc/get-connection {:dbtype "postgresql" :dbname "edgar"
                                         :host "localhost" :port 5432
                                         :user "postgres" :password "postgres"})]
    (submit-edgar-pg! conn (edgar/dataset "src/dev/resources/data/edgar"))))
