(ns xtdb.datasets.edgar.pg
  "Loads the EDGAR companyfacts dataset into a plain Postgres database — the
   fundamentals system-of-record half of the demo, distinct from the bitemporal
   XTDB sink.

   Postgres holds current state only: each statement row UPSERTs by its period
   key, keeping the most recently filed vintage. The restatement history (the
   prior, since-corrected values) and the instant valid-time timeline live in
   XTDB, not here — a real fundamentals SOR carries the latest figure, which is
   the point the federation demo makes.

   Schema is derived from the same statement-registry the parse layer uses, so
   the tables' line-item columns stay in lockstep with the concepts loaded."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.datasets.edgar :as edgar]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.datasets.edgar.tsv :as tsv]))

;; pgjdbc maps java.time.LocalDate ↔ date and BigDecimal ↔ numeric directly, and
;; the parse layer already yields those — so columns bind without coercion.

(defn- col-ddl [statement]
  (->> (parse/statement-column-names statement)
       (map #(str "      " (name %) " numeric"))
       (str/join ",\n")))

(def ^:private issuer-ddl
  "CREATE TABLE IF NOT EXISTS issuer (
      cik text PRIMARY KEY,
      entity_name text,
      filed date)")

;; income_statement is keyed by the full period (start, end) — a duration figure
;; is fixed for the window it reports. balance_sheet is keyed by the as-of date
;; (end) — an instant balance supersedes the prior as-of value.
(defn- income-statement-ddl []
  (format "CREATE TABLE IF NOT EXISTS income_statement (
      cik text,
      period_start date,
      period_end date,
%s,
      accession text,
      form text,
      filed date,
      PRIMARY KEY (cik, period_start, period_end))"
          (col-ddl :income_statement)))

(defn- balance-sheet-ddl []
  (format "CREATE TABLE IF NOT EXISTS balance_sheet (
      cik text,
      period_end date,
%s,
      accession text,
      form text,
      filed date,
      PRIMARY KEY (cik, period_end))"
          (col-ddl :balance_sheet)))

(defn create-tables!
  "Creates the EDGAR tables if absent. Idempotent."
  [conn]
  (doseq [stmt [issuer-ddl (income-statement-ddl) (balance-sheet-ddl)]]
    (jdbc/execute! conn [stmt])))

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

(defn submit-companyfacts-pg!
  "Loads one companyfacts file into Postgres via `conn`, UPSERTing current state."
  [conn {:keys [reader allow-set] :or {allow-set parse/demo-cik-allow-set}}]
  (with-open [rdr reader]
    (submit-docs! conn (parse/read-docs rdr allow-set))))

(defn submit-edgar-pg!
  "Loads EDGAR companyfacts files into Postgres via `conn` (a next.jdbc
   connectable), creating tables and UPSERTing current state. system-time is
   irrelevant here since Postgres keeps latest only."
  [conn {:keys [files allow-set] :or {allow-set parse/demo-cik-allow-set}}]
  (create-tables! conn)
  (log/info "Loading" (count files) "EDGAR companyfacts file(s) into Postgres...")
  (doseq [f files]
    (submit-companyfacts-pg! conn {:reader (edgar/->reader f) :allow-set allow-set})))

(defn submit-quarters-pg!
  "Loads EDGAR quarterly TSV dumps into Postgres, one quarter at a time (only one
   quarter held in memory). Each quarter is a [sub-file num-file] pair."
  [conn quarters]
  (create-tables! conn)
  (doseq [[sub-f num-f] quarters]
    (with-open [sub-rdr (edgar/->reader sub-f)
                num-rdr (edgar/->reader num-f)]
      (submit-docs! conn (tsv/quarter->docs sub-rdr num-rdr)))))

(comment
  ;; Run against a local Postgres (companyfacts files as produced by
  ;; scripts/download-edgar.sh):
  (with-open [conn (jdbc/get-connection {:dbtype "postgresql" :dbname "edgar"
                                         :host "localhost" :port 5432
                                         :user "postgres"})]
    (submit-edgar-pg! conn {:files [(clojure.java.io/file "data/edgar/CIK0000320193.json.gz")]})))
