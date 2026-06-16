(ns xtdb.datasets.gleif.pg
  "Loads the GLEIF LEI dataset into a plain Postgres database — the sec-master
   system-of-record half of the demo, distinct from the bitemporal XTDB sink.

   Postgres holds current state only: one `lei` row per entity (UPSERT by lei),
   and one row per event in a table per LegalEntityEventType, UPSERT by
   (lei, effective_date). It reflects the latest golden copy with no history —
   the bitemporal history lives in XTDB, not here, which is the point the
   federation demo makes.

   GLEIF re-sends each touched entity's full snapshot + complete event history on
   every publish, so the same event arrives verbatim across many deltas; the
   (lei, effective_date) PK collapses those re-sends. The entity's nested bits
   (addresses, other-names) ride along on the `lei` row as jsonb.

   Enums are native PG enum types whose values are the kebab strings the mirror
   produces (`active`, `issued`, `completed`), so PG and XT agree and the
   GleifPgIndexer's PG→keyword mapping is just `keyword`. An out-of-enum value
   fails the insert — surfacing unmodelled data rather than silently coercing."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [xtdb.datasets.gleif :as gleif])
  (:import [java.time Instant ZoneOffset]
           [java.util.zip GZIPInputStream]
           [org.postgresql.util PGobject]))

;; GLEIF's LegalEntityEvents split into two kinds (the authoritative 19 from the
;; LEI-CDF 3.1 XSD). An event of an unmapped type surfaces (load-file! throws)
;; rather than being silently dropped.

;; State transitions: the change carries only a date — the *value* lives on the
;; snapshot — so each row also stores the then-current attribute value (the
;; record only ever shows the one change that produced its current value, so the
;; pairing is 1:1). :value-col is the attribute column; :value-fn pulls it off
;; the record; :jsonb? routes it through ->jsonb. Mirrored into XT keyed by lei.
(def state-change-type->table
  {:change-legal-name    {:table "lei_legal_name_changes"    :value-col "legal_name"      :value-fn :legal-name}
   :change-other-names   {:table "lei_other_name_changes"    :value-col "other_names"     :value-fn :other-names   :jsonb? true}
   :change-legal-address {:table "lei_legal_address_changes" :value-col "legal_address"   :value-fn :legal-address :jsonb? true}
   :change-hq-address    {:table "lei_hq_address_changes"    :value-col "hq_address"      :value-fn :hq-address    :jsonb? true}
   :change-legal-form    {:table "lei_legal_form_changes"    :value-col "legal_form_code" :value-fn :legal-form-code}})

;; Occurrences: things that happened at a point, no value to carry. Pluralised
;; `lei_<thing>`. Mirrored into XT keyed by lei__effective_date.
(def occurrence-type->table
  {:demerger "lei_demergers"
   :spinoff "lei_spinoffs"
   :absorption "lei_absorptions"
   :acquisition-branch "lei_acquisition_branches"
   :transformation-branch-to-subsidiary "lei_transformation_branch_to_subsidiary"
   :transformation-subsidiary-to-branch "lei_transformation_subsidiary_to_branch"
   :transformation-umbrella-to-standalone "lei_transformation_umbrella_to_standalone"
   :breakup "lei_breakups"
   :mergers-and-acquisitions "lei_mergers_and_acquisitions"
   :bankruptcy "lei_bankruptcies"
   :liquidation "lei_liquidations"
   :voluntary-arrangement "lei_voluntary_arrangements"
   :insolvency "lei_insolvencies"
   :dissolution "lei_dissolutions"})

(defn- ->ts
  "pgjdbc maps OffsetDateTime ↔ timestamptz; it rejects java.time.Instant
   (which is what the mirror produces for the XTDB sink)."
  [v]
  (when v (.atOffset ^Instant v ZoneOffset/UTC)))

(defn- ->enum
  "A kebab keyword → the plain enum-label string, or nil. We bind enums as text
   and cast them in the SQL (`?::entity_category`) — pgjdbc can't resolve a custom
   enum type to bind a PGobject (it throws \"Unknown type …\"), but Postgres
   coerces text → enum server-side via the cast."
  [kw]
  (some-> kw name))

(defn- ->jsonb [v]
  (when v
    (doto (PGobject.) (.setType "jsonb") (.setValue (json/write-value-as-string v)))))

;;; --- upserts ------------------------------------------------------------------

(def ^:private lei-upsert
  "INSERT INTO lei
     (lei, legal_name, legal_address, hq_address, jurisdiction, legal_form_code,
      entity_category, entity_status, entity_creation_date, registration_status,
      initial_registration_date, last_update_date, next_renewal_date, managing_lou,
      other_names)
   VALUES (?,?,?,?,?,?,
           ?::entity_category,?::entity_status,?,?::registration_status,
           ?,?,?,?,?)
   ON CONFLICT (lei) DO UPDATE SET
     legal_name = EXCLUDED.legal_name, legal_address = EXCLUDED.legal_address,
     hq_address = EXCLUDED.hq_address, jurisdiction = EXCLUDED.jurisdiction,
     legal_form_code = EXCLUDED.legal_form_code,
     entity_category = EXCLUDED.entity_category, entity_status = EXCLUDED.entity_status,
     entity_creation_date = EXCLUDED.entity_creation_date,
     registration_status = EXCLUDED.registration_status,
     initial_registration_date = EXCLUDED.initial_registration_date,
     last_update_date = EXCLUDED.last_update_date,
     next_renewal_date = EXCLUDED.next_renewal_date, managing_lou = EXCLUDED.managing_lou,
     other_names = EXCLUDED.other_names")

(defn- lei-params [{:keys [registration] :as record}]
  [(:lei record) (:legal-name record) (->jsonb (:legal-address record)) (->jsonb (:hq-address record))
   (:jurisdiction record) (:legal-form-code record)
   (->enum (:entity-category record)) (->enum (:entity-status record))
   (->ts (:entity-creation-date record)) (->enum (:status registration))
   (->ts (:initial-date registration)) (->ts (:last-update registration)) (->ts (:next-renewal registration))
   (:managing-lou registration)
   (->jsonb (:other-names record))])

(defn- event-effective
  "An event's effective date, falling back to its recorded date. in-progress
   events have no effective-date yet (recorded but not legally in force); the
   fallback keeps the (lei, effective_date) PK non-null."
  [{:keys [effective-date recorded-date]}]
  (or effective-date recorded-date))

;;; occurrences — date-only

(defn- occurrence-upsert [table]
  (format "INSERT INTO %s
             (lei, effective_date, recorded_date, status, group_type, validation_documents)
           VALUES (?,?,?,?::event_status,?::event_group_type,?)
           ON CONFLICT (lei, effective_date) DO UPDATE SET
             recorded_date = EXCLUDED.recorded_date, status = EXCLUDED.status,
             group_type = EXCLUDED.group_type,
             validation_documents = EXCLUDED.validation_documents"
          table))

(defn- occurrence-params [lei {:keys [recorded-date status group-type validation-documents] :as ev}]
  [lei (->ts (event-effective ev)) (->ts recorded-date)
   (->enum status) (->enum group-type)
   (some-> validation-documents name)])

;;; state changes — carry the then-current attribute value off the record

(defn- state-change-upsert [{:keys [table value-col]}]
  (format "INSERT INTO %s
             (lei, effective_date, recorded_date, status, group_type, validation_documents, %s)
           VALUES (?,?,?,?::event_status,?::event_group_type,?,?)
           ON CONFLICT (lei, effective_date) DO UPDATE SET
             recorded_date = EXCLUDED.recorded_date, status = EXCLUDED.status,
             group_type = EXCLUDED.group_type,
             validation_documents = EXCLUDED.validation_documents,
             %s = EXCLUDED.%s"
          table value-col value-col value-col))

(defn- state-change-params [{:keys [value-fn jsonb?]} record {:keys [recorded-date status group-type validation-documents] :as ev}]
  (let [value (value-fn record)]
    [(:lei record) (->ts (event-effective ev)) (->ts recorded-date)
     (->enum status) (->enum group-type)
     (some-> validation-documents name)
     (if jsonb? (->jsonb value) value)]))

;;; --- load ---------------------------------------------------------------------

(defn- upsert-batch!
  "UPSERT `params` into `conn` in batches. `label` names the target for the
   per-batch debug log — the anchor is ~200 lei batches, so it's how you see it
   tick rather than stall silently."
  [conn label sql params batch-size]
  (->> (partition-all batch-size params)
       (reduce (fn [n batch]
                 (jdbc/with-transaction [tx conn]
                   (jdbc/execute-batch! tx sql batch {}))
                 (log/debugf "    %s — batch %d (%d rows)" label (inc n) (count batch))
                 (inc n))
               0)))

(defn- recorded-since?
  "Was this event recorded in (since, now]? `since` nil (the anchor) keeps every
   event. GLEIF re-sends an entity's whole event history on every publish, so we
   only insert an event in the file that first carries it — the one whose publish
   window contains its recorded-date — rather than re-upserting it ~20x."
  [since {:keys [recorded-date]}]
  (and recorded-date
       (or (nil? since) (pos? (compare recorded-date since)))))

(defn- dedup-by-pk
  "Within a file, a record can carry two same-type events on the same effective
   date, and ON CONFLICT can't touch the same PK twice in one batched statement.
   Keep one per (lei, effective_date) — they collapse anyway."
  [lei+params->pk lei+events ->params]
  (->> lei+events
       (into {} (map (fn [[ctx ev]] [(lei+params->pk ctx ev) (->params ctx ev)])))
       vals))

(defn- load-file! [conn file since batch-size]
  (with-open [in (-> (io/input-stream file) GZIPInputStream.)]
    (let [records (gleif/read-records in)
          ;; events newly recorded in this publish window, paired with their record
          ;; (state changes need the record's snapshot value), grouped by :type.
          by-type (->> records
                       (mapcat (fn [{:keys [events] :as record}]
                                 (for [ev events :when (recorded-since? since ev)] [record ev])))
                       (group-by (comp :type second)))]
      ;; the lei rows — current state, always upserted:
      (upsert-batch! conn "lei" lei-upsert (map lei-params records) batch-size)
      ;; the events, fanned out by type into their own tables. An unmapped type is
      ;; a data surprise — surface it rather than dropping the event.
      (doseq [[type rec+events] by-type]
        (cond
          (state-change-type->table type)
          (let [{:keys [table] :as spec} (state-change-type->table type)]
            (upsert-batch! conn table (state-change-upsert spec)
                           (dedup-by-pk (fn [record ev] [(:lei record) (event-effective ev)])
                                        rec+events
                                        (fn [record ev] (state-change-params spec record ev)))
                           batch-size))

          (occurrence-type->table type)
          (let [table (occurrence-type->table type)]
            (upsert-batch! conn table (occurrence-upsert table)
                           (dedup-by-pk (fn [record ev] [(:lei record) (event-effective ev)])
                                        rec+events
                                        (fn [record ev] (occurrence-params (:lei record) ev)))
                           batch-size))

          :else
          (throw (ex-info "unmapped GLEIF event type"
                          {:type type :file (.getName (io/file file))}))))
      (log/infof "  %s — %d records" (.getName (io/file file)) (count records)))))

(defn submit-gleif-pg!
  "Loads the GLEIF dataset into Postgres via `conn` (a next.jdbc connectable),
   UPSERTing current state from the mirrored transit files. system-time is
   irrelevant here since Postgres keeps latest.

   Files are loaded in publish order; each event is inserted only in the file
   whose publish window first carries it (by recorded-date), so we don't re-upsert
   an entity's whole event history on every publish it appears in.

   The schema (tables, enums, publication) is owned by the demo's
   init/postgres/01-gleif.sql — this loader only UPSERTs into it."
  [conn {:keys [anchor deltas batch-size] :or {batch-size 1000}}]
  (log/info "Loading GLEIF into Postgres — anchor +" (count deltas) "deltas...")
  ;; (since, date] window per file: `since` is the previous file's publish date,
  ;; nil for the anchor (which seeds every event it carries).
  (->> (concat (when anchor [anchor]) (sort-by :date deltas))
       (reduce (fn [since {:keys [file date]}]
                 ;; interruptible between files (~1000) — Thread/interrupted reads
                 ;; + clears the flag, leaving the thread clean for the next eval.
                 (when (Thread/interrupted)
                   (throw (InterruptedException. "interrupted loading GLEIF into Postgres")))
                 (load-file! conn file since batch-size)
                 date)
               nil))
  (log/info "GLEIF Postgres load complete."))

(comment
  ;; Load GLEIF into the demo Postgres (gleif/docker-compose.yml — db `gleif`,
  ;; schema owned by init/postgres/01-gleif.sql). Get the dataset first: sync it
  ;; down (scripts/download-dataset.sh --gleif) or produce it locally
  ;; (xtdb.datasets.gleif.mirror/mirror!) — both leave transit under the dir below.
  ;;
  ;; For the full federation demo (node + attach + watch CDC), see the comment at
  ;; the bottom of xtdb.datasets.gleif, and modules/datasets/gleif/README.md.
  (def conn
    (jdbc/get-connection {:dbtype "postgresql" :dbname "gleif"
                          :host "localhost" :port 5432
                          :user "postgres" :password "postgres"}))

  ;; per-file progress at :info; watch the ~200-batch anchor tick at :debug:
  (user/set-log-level! 'xtdb.datasets.gleif.pg :debug)

  (submit-gleif-pg! conn (gleif/dataset "src/test/resources/data/gleif"))

  ;; eyeball the result (psql):
  ;;   SELECT count(*) FROM lei;
  ;;   SELECT lei, entity_status, legal_address->>'city' FROM lei LIMIT 5;
  ;;   SELECT lei, effective_date, legal_name FROM lei_legal_name_changes LIMIT 5;

  (.close conn))
