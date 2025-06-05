(ns xtdb.information-schema
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [xtdb.authn.crypt :as authn.crypt]
            [xtdb.metadata]
            [xtdb.pgwire.types :as pg-types]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (io.micrometer.core.instrument Counter Gauge MeterRegistry Tag Timer)
           (io.micrometer.core.instrument.distribution ValueAtPercentile)
           [java.time Duration]
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb ICursor)
           xtdb.api.query.IKeyFn
           (xtdb.arrow Relation RelationReader VectorReader VectorWriter)
           (xtdb.indexer Watermark)
           xtdb.operator.SelectionSpec
           (xtdb.trie MemoryHashTrie Trie TrieCatalog)
           xtdb.types.Fields))

(defn name->oid [s]
  (Math/abs ^Integer (hash s)))

(defn schema-info->col-rows [schema-info]
  (for [table-entry schema-info
        :let [table (key table-entry)
              cols (into (when-not (and (contains? #{"pg_catalog" "information_schema" "xt"} (namespace table))
                                        (not= 'xt/txs table))
                           {"_valid_from" (-> Fields/TEMPORAL (.toArrowField "_valid_from"))
                            "_valid_to" (-> Fields/TEMPORAL (.getNullable) (.toArrowField "_valid_to"))
                            "_system_from" (-> Fields/TEMPORAL (.toArrowField "_system_from"))
                            "_system_to" (-> Fields/TEMPORAL (.getNullable) (.toArrowField "_system_to"))})
                         (val table-entry))
              {xt-cols true, user-cols false} (group-by (comp #(str/starts-with? % "_") key) cols)
              cols (concat (sort-by key xt-cols)
                           (sort-by key user-cols))]
        [idx col] (map-indexed #(vector %1 %2) cols)
        :let [name (key col)
              col-field (val col)]]

    {:idx (inc idx) ;; no guarentee of stability of idx for a given col
     :table table
     :name name
     :field col-field
     :type (types/field->col-type col-field)}))

(do
  (def info-tables
    '{information_schema/tables {table_catalog :utf8, table_schema :utf8, table_name :utf8, table_type :utf8}
      information_schema/columns {table_catalog :utf8, table_schema :utf8, table_name :utf8, column_name :utf8, data_type :utf8}
      information_schema/schemata {catalog_name :utf8, schema_name :utf8, schema_owner :utf8}})

  (def ^:private pg-catalog-tables
    '{pg_catalog/pg_tables {schemaname :utf8, tablename :utf8, tableowner :utf8, tablespace :null}
      pg_catalog/pg_type {oid :i32, typname :utf8, typnamespace :i32, typowner :i32
                          typtype :utf8, typbasetype :i32, typnotnull :bool, typtypmod :i32
                          typsend :utf8, typreceive :utf8, typinput :utf8, typoutput :utf8
                          typrelid :i32, typelem :i32}
      pg_catalog/pg_class {oid :i32, relname :utf8, relnamespace :i32, relkind :utf8, relam :i32, relchecks :i32
                           relhasindex :bool, relhasrules :bool, relhastriggers :bool, relrowsecurity :bool
                           relforcerowsecurity :bool, relispartition :bool, reltablespace :i32, reloftype :i32
                           relpersistence :utf8, relreplident :utf8, reltoastrelid :i32}
      pg_catalog/pg_description {objoid :i32, classoid :i32, objsubid :i16, description :utf8}
      pg_catalog/pg_views {schemaname :utf8, viewname :utf8, viewowner :utf8}
      pg_catalog/pg_matviews {schemaname :utf8, matviewname :utf8, matviewowner :utf8}
      pg_catalog/pg_attribute {attrelid :i32, attname :utf8, atttypid :i32
                               attlen :i32, attnum :i32
                               attisdropped :bool, attnotnull :bool
                               atttypmod :i32, attidentity :utf8, attgenerated :utf8}
      pg_catalog/pg_namespace {oid :i32, nspname :utf8, nspowner :i32, nspacl :null}
      pg_catalog/pg_proc {oid :i32, proname :utf8, pronamespace :i32}
      pg_catalog/pg_database {oid :i32, datname :utf8, datallowconn :bool, datistemplate :bool}

      pg_catalog/pg_stat_user_tables {relid :i32
                                      schemaname :utf8
                                      relname :utf8
                                      n_live_tup :i64}

      pg_catalog/pg_settings {name :utf8, setting :utf8}

      pg_catalog/pg_range {rngtypid :i32, rngsubtype :i32, rngmultitypid :i32
                           rngcollation :i32, rngsubopc :i32, rngcanonical :utf8, rngsubdiff :utf8}
      pg_catalog/pg_am {oid :i32, amname :utf8, amhandler :utf8, amtype :utf8}})

  (def ^:private pg-catalog-template-tables
    '{pg_catalog/pg_user {username :utf8 usesuper :bool passwd [:union #{:utf8 :null}]}})

  (def ^:private xt-derived-tables
    '{xt/trie_stats {schema_name :utf8, table_name :utf8, trie_key :utf8, level :i32, recency [:union #{:null [:date :day]}],
                     trie_state :utf8, data_file_size :i64, row_count [:union #{:null :i64}], temporal_metadata [:union #{:null [:struct {}]}]}

      xt/live_tables {schema_name :utf8, table_name :utf8, row_count :i64}

      xt/live_columns {schema_name :utf8, table_name :utf8, col_name :utf8, col_type :utf8}

      xt/metrics_timers {name :utf8, tags [:struct {}]
                         count :i64,
                         mean_time [:union #{:null [:duration :nano]}],
                         p75_time [:union #{:null [:duration :nano]}]
                         p95_time [:union #{:null [:duration :nano]}]
                         p99_time [:union #{:null [:duration :nano]}]
                         p999_time [:union #{:null [:duration :nano]}]
                         max_time [:union #{:null [:duration :nano]}]}

      xt/metrics_gauges {name :utf8, tags [:struct {}], value :f64}
      xt/metrics_counters {name :utf8, tags [:struct {}], count :f64}})

  (def derived-tables
    (-> (merge info-tables pg-catalog-tables xt-derived-tables)
        (update-vals (fn [col-types]
                       (->> (for [[col-name col-type] col-types]
                              [col-name (types/col-type->field col-name col-type)])
                            (into {}))))))

  (def template-tables
    (-> pg-catalog-template-tables
        (update-vals (fn [col-types]
                       (->> (for [[col-name col-type] col-types]
                              [col-name (types/col-type->field col-name col-type)])
                            (into {}))))))

  (def table-info (-> (merge derived-tables template-tables) (update-vals (comp set keys))))

  (def unq-pg-catalog
    (-> (merge pg-catalog-tables pg-catalog-template-tables)
        (update-vals keys)
        (update-keys (comp symbol name))))

  (def meta-table-schemas
    (-> (merge info-tables pg-catalog-tables pg-catalog-template-tables)
        (update-vals (fn [col-name->type]
                       (-> col-name->type
                           (update-keys str)
                           (update-vals #(types/col-type->field %)))))))

  (def views (-> (set (keys meta-table-schemas))
                 (disj 'pg_catalog/pg_user))))



(def schemas
  [{:catalog-name "xtdb"
    :schema-name "pg_catalog"
    :schema-owner "xtdb"}
   {:catalog-name "xtdb"
    :schema-name "public"
    :schema-owner "xtdb"}
   {:catalog-name "xtdb"
    :schema-name "information_schema"
    :schema-owner "xtdb"}])

(def pg-namespaces
  (mapv (fn [{:keys [schema-owner schema-name] :as schema}]
          (-> schema
              (assoc :nspowner (name->oid schema-owner))
              (assoc :oid (name->oid schema-name))))
        schemas))

(defn tables [schema-info]
  (for [table (keys schema-info)]
    {:table-catalog "xtdb"
     :table-name (name table)
     :table-schema (namespace table)
     :table-type (if (views table) "VIEW" "BASE TABLE")}))

(defn pg-tables [schema-info]
  (for [table (keys schema-info)]
    {:schemaname (namespace table)
     :tablename (name table)
     :tableowner "xtdb"
     :tablespace nil}))

(defn pg-class [schema-info]
  (for [table (keys schema-info)]
    {:oid (name->oid table)
     :relname (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) (name table))
     :relnamespace (name->oid (namespace table))
     :relkind "r"
     :relam (int 2)
     :relchecks (int 0)
     :relhasindex true
     :relhasrules false
     :relhastriggers false
     :relrowsecurity false
     :relforcerowsecurity false
     :relispartition false
     :reltablespace (int 0)
     :reloftype (int 0)
     :relpersistence "p"
     :relreplident "d"
     :reltoastrelid (int 0)}))

(defn pg-type []
  (for [{:keys [oid typname typsend typreceive typelem typinput typoutput]} (vals (dissoc pg-types/pg-types :default :null))]
    {:oid (int oid)
     :typname typname
     :typnamespace (name->oid "pg_catalog")
     :typowner (name->oid "xtdb")
     :typtype "b"
     :typbasetype (int 0)
     :typnotnull false
     :typtypmod (int -1)
     :typrelid (int 0) ; zero for non composite types (or pg_class ref for composite types)
     :typelem (int (or typelem 0))
     :typsend typsend
     :typreceive typreceive
     :typinput (or typinput "")
     :typoutput (or typoutput "")}))

(defn pg-range []
  (for [{:keys [rngtypid rngsubtype rngmultitypid rngcollation rngsubopc rngcanonical rngsubdiff]} (vals pg-types/pg-ranges)]
    {:rngtypid (int rngtypid)
     :rngsubtype (int rngsubtype)
     :rngmultitypid (int rngmultitypid)
     :rngcollation (int rngcollation)
     :rngsubopc (int rngsubopc)
     :rngcanonical rngcanonical
     :rngsubdiff rngsubdiff}))

(defn columns [col-rows]
  (for [{:keys [table type], col-name :name} col-rows]
    {:table-catalog "xtdb"
     :table-name (name table)
     :table-schema (namespace table)
     :column-name (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) col-name)
     :data-type (pr-str type)}))

(defn pg-attribute [col-rows]
  (for [{:keys [idx table name field]} col-rows
        :let [{:keys [oid typlen]} (-> field
                                       (types/field-with-name name)
                                       (pg-types/field->pg-col)
                                       :pg-type
                                       (->> (get (-> pg-types/pg-types
                                                     (assoc :default (get pg-types/pg-types :json))))))]]
    {:attrelid (name->oid table)
     :attname (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) name)
     :atttypid (int oid)
     :attlen (int (or typlen -1))
     :attnum (int idx)
     :attisdropped false
     :attnotnull false
     :atttypmod (int -1)
     :attidentity ""
     :attgenerated ""}))

(defn pg-namespace []
  (for [{:keys [oid schema-name nspowner]} pg-namespaces]
    {:oid oid
     :nspname schema-name
     :nspowner nspowner
     :nspacl nil}))

(def pg-ams [{:oid 2
              :amname "heap"
              :amhandler "heap_tableam_handler"
              :amtype "t"}
             {:oid 403
              :amname "btree"
              :amhandler "bthandler"
              :amtype "i"}
             {:oid 405
              :amname "hash"
              :amhandler "hashhandler"
              :amtype "i"}])

(defn pg-am []
  (for [{:keys [oid amname amhandler amtype]} pg-ams]
    {:oid (int oid)
     :amname amname
     :amhandler amhandler
     :amtype amtype}))

(do ; to eval them both
  (def procs
    {'pg_catalog/array_in {:oid (name->oid "array_in")
                           :proname "array_in"
                           :pronamespace (name->oid "pg_catalog")}})

  (def oid->proc
    (into {} (map (juxt (comp :oid val) key) procs))))

(defn pg-proc []
  (for [{:keys [oid proname pronamespace]} (vals procs)]
    {:oid oid
     :proname proname
     :pronamespace pronamespace}))

(def databases
  [{:datname "xtdb"}])

(defn pg-database []
  (for [{:keys [datname]} databases]
    {:oid (name->oid datname)
     :datname datname
     :datallowconn true
     :datistemplate false}))

(defn pg-stat-user-tables [schema-info]
  (for [table (keys schema-info)]
    {:relid (name->oid (name table))
     :relname (name table)
     :schemaname (namespace table)
     :n-live-tup 0}))

(defn pg-settings []
  (for [[setting-name setting] {"max_index_keys" "32"}]
    {:name setting-name
     :setting setting}))

(def ^:private pg-user-field
  (types/col-type->field "put" [:struct '{_id :utf8
                                          username :utf8
                                          usesuper :bool
                                          passwd [:union #{:utf8 :null}]}]))

(def ^:private initial-user-data
  [{:_id "xtdb", :username "xtdb", :usesuper true, :passwd (authn.crypt/encrypt-pw "xtdb")}])

(defn pg-user-template-page+trie [allocator]
  (util/with-close-on-catch [out-rel-writer (Trie/openLogDataWriter allocator (Trie/dataRelSchema pg-user-field))]
    (let [{^VectorWriter iid-wrt "_iid",
           ^VectorWriter sys-wrt "_system_from"
           ^VectorWriter vf-wrt "_valid_from",
           ^VectorWriter vt-wrt "_valid_to"} out-rel-writer

          ^VectorWriter put-wrt (get-in out-rel-writer ["op" "put"])]

      (doseq [user initial-user-data]
        (.writeObject iid-wrt (util/->iid (:_id user)))
        (.writeLong sys-wrt 0)
        (.writeLong vf-wrt 0)
        (.writeLong vt-wrt Long/MAX_VALUE)
        (.writeObject put-wrt user)
        (.endRow out-rel-writer))
      (let [out-rel (vw/rel-wtr->rdr out-rel-writer)
            iid-rdr (vw/vec-wtr->rdr iid-wrt)
            dummy-trie (reduce #(.plus ^MemoryHashTrie %1 %2) (trie/->live-trie 32 1024 iid-rdr) (range (count initial-user-data)))]
        [out-rel dummy-trie]))))

(defn table->template-rel+tries [allocator]
  {'pg_catalog/pg_user (pg-user-template-page+trie allocator)})

(defn- split-table-name [fq-table-name]
  (let [fq-table (symbol fq-table-name)
        table-name (name fq-table)
        schema-name (namespace fq-table)]
    {:schema-name schema-name
     :table-name table-name}))

(defn trie-stats [^TrieCatalog trie-catalog]
  (for [table-name (.getTableNames trie-catalog)
        :let [trie-state (trie-cat/trie-state trie-catalog table-name)]
        {:keys [trie-key level recency state data-file-size trie-metadata]} (trie-cat/all-tries trie-state)
        :let [{:keys [row-count] :as trie-meta} (some-> trie-metadata trie-cat/<-trie-metadata)]]
    (into (split-table-name table-name)
          {:trie-key trie-key, :level (int level), :recency recency, :data-file-size data-file-size
           :trie-state (name state), :row-count row-count, :temporal-metadata (some-> trie-meta (dissoc :row-count :iid-bloom))})))

(defn live-tables [^Watermark wm]
  (let [li-wm (.getLiveIndex wm)]
    (for [table (.getLiveTables li-wm)
          :let [live-table (.liveTable li-wm table)]]
      (into (split-table-name table)
            {:row-count (long (.getRowCount (.getLiveRelation live-table)))}))))

(defn live-columns [^Watermark wm]
  (let [li-wm (.getLiveIndex wm)]
    (for [table (.getLiveTables li-wm)
          [col-name col-field] (.getColumnFields (.liveTable li-wm table))]
      (into (split-table-name table)
            {:col-name col-name
             :col-type (pr-str (types/field->col-type col-field))}))))

(defn metrics-timers [^MeterRegistry reg]
  (->> (.getMeters reg)
       (filter #(instance? Timer %))
       (mapv (fn [^Timer timer]
               (let [snapshot (.takeSnapshot timer)
                     id (.getId timer)]
                 (into {:name (.getName id)
                        :tags (->> (.getTags id)
                                   (into {} (map (juxt #(.getKey ^Tag %) #(.getValue ^Tag %)))))
                        :count (.count snapshot)
                        :mean-time (Duration/ofNanos (.mean snapshot))
                        :max-time (Duration/ofNanos (.max snapshot))}
                       (keep (fn [^ValueAtPercentile pv]
                               (case (.percentile pv)
                                 0.75 [:p75-time (Duration/ofNanos (.value pv))]
                                 0.95 [:p95-time (Duration/ofNanos (.value pv))]
                                 0.99 [:p99-time (Duration/ofNanos (.value pv))]
                                 0.999 [:p999-time (Duration/ofNanos (.value pv))]
                                 nil)))
                       (.percentileValues snapshot)))))))

(defn metrics-gauges [^MeterRegistry reg]
  (->> (.getMeters reg)
       (filter #(instance? Gauge %))
       (mapv (fn [^Gauge gauge]
               (let [id (.getId gauge)]
                 {:name (.getName id)
                  :tags (->> (.getTags id)
                             (into {} (map (juxt #(.getKey ^Tag %) #(.getValue ^Tag %)))))
                  :value (.value gauge)})))))

(defn metrics-counters [^MeterRegistry reg]
  (->> (.getMeters reg)
       (filter #(instance? Counter %))
       (mapv (fn [^Counter counter]
               (let [id (.getId counter)]
                 {:name (.getName id)
                  :tags (->> (.getTags id)
                             (into {} (map (juxt #(.getKey ^Tag %) #(.getValue ^Tag %)))))
                  :count (.count counter)})))))

(deftype InformationSchemaCursor [^:unsynchronized-mutable ^RelationReader out-rel vsr]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-let [^RelationReader out-rel (.out_rel this)]
       (try
         (set! (.out-rel this) nil)
         (.accept c out-rel)
         true
         (finally
           (util/close vsr)
           (.close out-rel))))))

  (close [_]
    (util/close vsr)
    (some-> out-rel .close)))

(defprotocol InfoSchema
  (->cursor [info-schema allocator wm derived-table-schema
             table col-names col-preds
             schema params]))

(defmethod ig/prep-key :xtdb/information-schema [_ opts]
  (into {:table-catalog (ig/ref :xtdb/table-catalog)
         :trie-catalog (ig/ref :xtdb/trie-catalog)
         :metrics-registry (ig/ref :xtdb.metrics/registry)}
        opts))

(defmethod ig/init-key :xtdb/information-schema [_ {:keys [table-catalog trie-catalog metrics-registry]}]
  (reify InfoSchema
    (->cursor [_ allocator wm derived-table-schema
               table col-names col-preds
               schema params]
      ;;TODO should use the schema passed to it, but also regular merge is insufficient here for colFields
      ;;should be types/merge-fields as per scan-fields
      (let [schema-info (-> (merge-with merge
                                        (table-cat/all-column-fields table-catalog)
                                        (some-> (.getLiveIndex ^Watermark wm)
                                                (.getAllColumnFields)))
                            (update-keys symbol)
                            (merge meta-table-schemas))]
        (util/with-close-on-catch [out-root (util/with-open [out-rel (Relation/open ^BufferAllocator allocator
                                                                                    (Schema. (vec (vals derived-table-schema))))]

                                              (.writeRows out-rel (->> (case table
                                                                         information_schema/tables (tables schema-info)
                                                                         information_schema/columns (columns (schema-info->col-rows schema-info))
                                                                         information_schema/schemata schemas
                                                                         pg_catalog/pg_tables (pg-tables schema-info)
                                                                         pg_catalog/pg_type (pg-type)
                                                                         pg_catalog/pg_class (pg-class schema-info)
                                                                         pg_catalog/pg_description nil
                                                                         pg_catalog/pg_views nil
                                                                         pg_catalog/pg_matviews nil
                                                                         pg_catalog/pg_attribute (pg-attribute (schema-info->col-rows schema-info))
                                                                         pg_catalog/pg_namespace (pg-namespace)
                                                                         pg_catalog/pg_proc (pg-proc)
                                                                         pg_catalog/pg_database (pg-database)
                                                                         pg_catalog/pg_stat_user_tables (pg-stat-user-tables schema-info)
                                                                         pg_catalog/pg_settings (pg-settings)
                                                                         pg_catalog/pg_range (pg-range)
                                                                         pg_catalog/pg_am (pg-am)
                                                                         xt/trie_stats (trie-stats trie-catalog)
                                                                         xt/live_tables (live-tables wm)
                                                                         xt/live_columns (live-columns wm)
                                                                         xt/metrics_timers (metrics-timers metrics-registry)
                                                                         xt/metrics_gauges (metrics-gauges metrics-registry)
                                                                         xt/metrics_counters (metrics-counters metrics-registry)
                                                                         (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table))))
                                                                       (into-array java.util.Map)))
                                              (.openAsRoot out-rel allocator))]
          (assert out-root)
          (let [out-rel (vr/<-root out-root)]
            ;;TODO reuse relation selector code from trie cursor
            (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                                (.select rel (.select col-pred allocator rel schema params)))
                                              (-> out-rel
                                                  (->> (filter (comp (set col-names) #(.getName ^VectorReader %))))
                                                  (vr/rel-reader (.getRowCount out-rel))
                                                  (vr/with-absent-cols allocator col-names))
                                              (vals col-preds))
                                      out-root)))))))
