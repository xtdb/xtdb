(ns xtdb.datasets.edgar.parse
  "Pure parsing of SEC EDGAR companyfacts JSON into XTDB doc maps.

   Deliberately free of any xtdb.api dependency: the same record->docs transform
   feeds the bitemporal XTDB sink and the current-state Postgres sink.

   The companyfacts document (data.sec.gov/api/xbrl/companyfacts/CIK{n}.json)
   nests facts/<taxonomy>/<concept>/units/<unit>/[observation]. We project that
   into three shapes by the *temporality* of the data, not by how it was filed:

   - issuer            — static reference (cik, entity name). No period.
   - income_statement  — duration facts (flows: revenue, net income). A figure
                         is fixed for the period it reports (start..end); a
                         restatement is a new *belief* about it, so valid-from is
                         `filed` and corrections show on the system-time axis.
   - balance_sheet     — instant facts (balances: assets, shares outstanding).
                         A value is as-of a date (`end`), so a later as-of date
                         supersedes the earlier *in valid-time* — valid-from is
                         `end`, a real valid-time timeline.

   Each statement is a wide row: observations for one (cik, period, filing) are
   pivoted so columns are the line items. A restatement re-files the same
   (cik, period) under a new accession with corrected line items."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [jsonista.core :as json])
  (:import [java.time LocalDate ZoneOffset]
           [java.time.format DateTimeParseException]
           [java.util.zip GZIPInputStream]))

(defn gz-reader ^java.io.Reader [file]
  (-> (io/input-stream file) GZIPInputStream. io/reader))

(def demo-cik-allow-set
  "Recognisable issuers whose companyfacts carry real restatements, keyed by the
   10-digit zero-padded CIK the JSON download uses. Apple is the anchor — its
   FY2008/2009 net income was retrospectively restated (10-K/A, 2010-01-25), so
   the same period appears under multiple `filed` vintages with divergent values:
   the bitemporal correction, in real data.

   When expanding, prefer issuers with known restatement history — the demo is
   weaker for a company that has never refiled."
  #{"0000320193"    ; Apple Inc. (FY08/09 net-income restatement)
    "0000040545"    ; General Electric Co
    "0001657853"    ; Hertz Global Holdings, Inc
    "0001336917"    ; Under Armour, Inc.
    "0000072971"}); Wells Fargo & Company

(defn snake-case
  "XBRL concepts are PascalCase (NetIncomeLoss); columns are snake_case. We don't
   try to reconcile near-synonym concepts (Revenues vs RevenueFromContract…) —
   each registered concept is its own column, verbatim-but-snake-cased."
  [concept]
  (-> concept
      (str/replace #"([a-z0-9])([A-Z])" "$1_$2")
      (str/replace #"([A-Z]+)([A-Z][a-z])" "$1_$2")
      str/lower-case))

(def statement-registry
  "The curated (taxonomy, concept) → statement mapping. Acts as the concept
   allow-set: unlisted concepts are dropped (an issuer reports thousands of XBRL
   tags; the wide statement tables carry only these). It is the *only* filter —
   every filer is loaded, just projected onto these recognisable line items.

   :statement is the target table; :period the concept's XBRL periodType (instant
   balances vs duration flows), which decides identity and valid-from. Column name
   is the snake-cased concept; near-synonym tags (Revenues vs RevenueFromContract…)
   are kept as distinct columns rather than reconciled."
  {;; income_statement — duration (flows). PK (cik, start, end), valid-from=filed.
   ["us-gaap" "Revenues"]                                          {:statement :income_statement, :period :duration}
   ["us-gaap" "RevenueFromContractWithCustomerExcludingAssessedTax"] {:statement :income_statement, :period :duration}
   ["us-gaap" "GrossProfit"]                                       {:statement :income_statement, :period :duration}
   ["us-gaap" "OperatingIncomeLoss"]                               {:statement :income_statement, :period :duration}
   ["us-gaap" "NetIncomeLoss"]                                     {:statement :income_statement, :period :duration}
   ["us-gaap" "ResearchAndDevelopmentExpense"]                     {:statement :income_statement, :period :duration}
   ["us-gaap" "NetCashProvidedByUsedInOperatingActivities"]        {:statement :income_statement, :period :duration}

   ;; balance_sheet — instant (balances). PK (cik, end), valid-from=end.
   ["us-gaap" "Assets"]                                            {:statement :balance_sheet, :period :instant}
   ["us-gaap" "AssetsCurrent"]                                     {:statement :balance_sheet, :period :instant}
   ["us-gaap" "Liabilities"]                                       {:statement :balance_sheet, :period :instant}
   ["us-gaap" "LiabilitiesCurrent"]                                {:statement :balance_sheet, :period :instant}
   ["us-gaap" "StockholdersEquity"]                                {:statement :balance_sheet, :period :instant}
   ["us-gaap" "Goodwill"]                                          {:statement :balance_sheet, :period :instant}
   ["us-gaap" "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"] {:statement :balance_sheet, :period :instant}
   ["us-gaap" "CommonStockSharesOutstanding"]                      {:statement :balance_sheet, :period :instant}
   ["us-gaap" "CommonStockSharesIssued"]                           {:statement :balance_sheet, :period :instant}
   ["dei" "EntityCommonStockSharesOutstanding"]                    {:statement :balance_sheet, :period :instant}})

(def ^:private statement-columns
  "Per-statement set of snake-cased column names the registry can produce — used
   so a pivoted row has a stable, predictable shape (the DDL knows the columns)."
  (->> statement-registry
       (reduce (fn [acc [[_tax concept] {:keys [statement]}]]
                 (update acc statement (fnil conj #{}) (snake-case concept)))
               {})))

(defn statement-column-names
  "The column keywords a given statement table carries (e.g. :revenues), so the
   Postgres DDL and the sinks agree on shape."
  [statement]
  (->> (get statement-columns statement) sort (mapv keyword)))

(defn cik->padded
  "EDGAR's companyfacts path uses a 10-digit zero-padded CIK; the tickers file
   gives a bare integer. Normalise to the padded form the allow-set keys on."
  [cik]
  (format "%010d" (long (if (string? cik) (Long/parseLong (str/trim cik)) cik))))

;; jsonista, keys left as strings: facts/<tax>/<concept>/units/<unit> are dynamic
;; (concept and unit names vary per filer), so string keys throughout.
(def ^:private mapper
  (json/object-mapper {:decode-key-fn false}))

(defn read-companyfacts
  "Parse a companyfacts JSON reader/stream into the raw nested map."
  [in]
  (json/read-value in mapper))

(defn- parse-date ^LocalDate [s]
  ;; EDGAR dates are ISO LocalDate ("2008-09-27"). A genuinely unparseable value
  ;; warns and yields nil rather than aborting the load on one bad row.
  (when-not (str/blank? s)
    (try
      (LocalDate/parse s)
      (catch DateTimeParseException _
        (log/warn "unparseable EDGAR date, skipping value:" (pr-str s))
        nil))))

(defn- ->instant
  "Dates in the source; XTDB wants an instant. Start of day in UTC — the
   resolution we have, made explicit."
  [^LocalDate d]
  (when d (.toInstant (.atStartOfDay d ZoneOffset/UTC))))

(defn ->decimal
  "Fundamental values are money/counts that must stay exact: coerce to BigDecimal
   so XT and the Postgres `numeric` column agree, and large or fractional values
   don't drift through a double. Accepts the companyfacts JSON number and the TSV
   reader's string form (\"958000000.0000\") alike; blank/unparseable → nil."
  [v]
  (cond
    (number? v) (bigdec v)
    (and (string? v) (not (str/blank? v))) (try (bigdec (str/trim v))
                                                (catch NumberFormatException _ nil))
    :else nil))

(defn- registered-observations
  "Flatten the companyfacts tree to the observations whose (taxonomy, concept) is
   in the registry, tagging each with its target statement, period type, and
   column. Drops anything unregistered or missing the dates it needs."
  [facts]
  (for [[taxonomy concepts] facts
        [concept concept-body] concepts
        :let [reg (get statement-registry [taxonomy concept])]
        :when reg
        [_unit observations] (get concept-body "units")
        obs observations
        :let [{:keys [statement period]} reg
              period-start (parse-date (get obs "start"))
              period-end (parse-date (get obs "end"))
              filed (parse-date (get obs "filed"))
              accn (get obs "accn")]
        ;; every fact needs its period end, an accession, and a filing date (the
        ;; system-time anchor); duration facts additionally need a start.
        :when (and period-end accn filed
                   (or (= period :instant) period-start))]
    {:statement statement
     :period period
     :column (keyword (snake-case concept))
     :period-start period-start
     :period-end period-end
     :filed filed
     :accession accn
     :form (get obs "form")
     :fiscal-year (get obs "fy")
     :fiscal-period (get obs "fp")
     :value (->decimal (get obs "val"))}))

(defn- pivot-statement
  "Pivot the observations of one statement-vintage — same (cik, statement,
   period, accession) — into a single wide doc, columns = line items.

   id is the *fact* identity, NOT the vintage: it excludes accession, so a
   restatement (a re-filing of the same period) shares the id and supersedes the
   prior in system-time. id = (cik, statement, [start,] end), delimiter-joined
   for now; once XT has composite primary keys this should become the key tuple
   directly. period-end renders via LocalDate's canonical ISO-8601 toString
   (stable); the joined form carries the usual delimiter caveat until then.

   valid-from differs by temporality:
   - duration: `filed` — the figure is fixed for its period, a restatement is a
     new belief (system-time).
   - instant: `period-end` — the balance is as-of that date; distinct as-of
     dates are distinct ids, giving a real valid-time timeline, while a
     re-report of the same date supersedes in system-time."
  [cik statement period obs-group]
  (let [{:keys [period-start period-end filed accession form fiscal-year
                fiscal-period]} (first obs-group)
        cols (into {} (map (juxt :column :value)) obs-group)
        id (str cik "__" (name statement) "__"
                (when period-start (str period-start "__")) period-end)]
    (with-meta
      (merge cols
             {:xt/id id
              :cik cik
              :period-start period-start
              :period-end period-end
              :accession accession
              :form form
              :fiscal-year fiscal-year
              :fiscal-period fiscal-period
              :filed filed
              :valid-from (->instant (if (= period :duration) filed period-end))})
      {:table statement})))

(defn observations->statements
  "A seq of normalised observation maps (the shape `registered-observations` and
   the TSV reader both yield: :statement/:period/:column/:period-start/
   :period-end/:filed/:accession/:form/:fiscal-year/:fiscal-period/:value) for a
   single cik → wide statement docs, one per (statement, period, accession)
   vintage. Shared by the companyfacts and TSV front-ends."
  [cik observations]
  (->> observations
       (group-by (juxt :statement :period-start :period-end :accession))
       (map (fn [[[statement _ps _pe _accn] obs-group]]
              (pivot-statement cik statement (:period (first obs-group)) obs-group)))))

(defn ->issuer-doc
  "The static issuer reference doc, derived from the issuer's earliest filing —
   that's when this identity first becomes known (valid-from), and the doc rides
   in that filing's transaction (it carries the same :accession + :filed, so the
   sink's group-by-accession keeps it atomic with that filing). A later filing
   reporting a different entity-name versions it in system-time."
  [cik entity-name earliest-observation]
  (let [{:keys [accession ^LocalDate filed]} earliest-observation]
    (with-meta
      {:xt/id cik
       :cik cik
       :entity-name entity-name
       :accession accession
       :filed filed
       :valid-from (->instant filed)}
      {:table :issuer})))

(defn record->docs
  "A parsed companyfacts map → a seq of docs (each tagged with its :table), or
   nil if curated out by allow-set. Yields the issuer then the statement docs."
  [allow-set parsed]
  (let [cik (cik->padded (get parsed "cik"))]
    (when (or (nil? allow-set) (contains? allow-set cik))
      (let [observations (registered-observations (get parsed "facts"))
            earliest (->> observations (sort-by :filed) first)]
        (cons (->issuer-doc cik (get parsed "entityName") earliest)
              (observations->statements cik observations))))))

(defn read-docs
  "Docs from a companyfacts JSON reader, applying record->docs + curation.
   Realised eagerly: callers read inside a with-open, so no lazy seq may outlive
   the reader."
  [reader allow-set]
  (vec (record->docs allow-set (read-companyfacts reader))))
