(ns xtdb.information-schema
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [xtdb.authn.crypt :as authn.crypt]
            [xtdb.metadata]
            [xtdb.serde.types :as st]
            [xtdb.table :as table]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine)
           (io.micrometer.core.instrument Counter Gauge MeterRegistry Tag Timer)
           (io.micrometer.core.instrument.distribution ValueAtPercentile)
           [java.lang AutoCloseable]
           [java.time Duration]
           [java.util Map]
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           xtdb.api.query.IKeyFn
           (xtdb.arrow Relation RelationReader VectorType VectorReader VectorWriter)
           xtdb.pgwire.PgType
           (xtdb.indexer Snapshot)
           xtdb.operator.SelectionSpec
           xtdb.table.TableRef
           (xtdb.trie MemoryHashTrie Trie TrieCatalog)))

(defn name->oid [s]
  (Math/abs ^Integer (hash s)))

(defn- map->vec-types [m]
  (update-vals m types/->type))

(defn schema-info->col-rows [schema-info]
  (for [[^TableRef table cols] schema-info
        :let [cols (into (when-not (and (contains? #{"pg_catalog" "information_schema" "xt"} (.getSchemaName table))
                                        (not= 'xt/txs (table/ref->schema+table table)))
                           (-> '{"_valid_from" :instant
                                 "_valid_to" [:? :instant]
                                 "_system_from" :instant
                                 "_system_to" [:? :instant]}
                               map->vec-types))
                         cols)
              {xt-cols true, user-cols false} (group-by (comp #(str/starts-with? % "_") key) cols)
              cols (concat (sort-by key xt-cols)
                           (sort-by key user-cols))]
        [idx col] (map-indexed #(vector %1 %2) cols)
        :let [name (key col)
              vec-type (val col)]]

    {:idx (inc idx) ;; no guarantee of stability of idx for a given col
     :table table
     :name name
     :vec-type vec-type}))

(do
  (def info-tables
    (-> '{information_schema/tables {table_catalog :utf8, table_schema :utf8, table_name :utf8, table_type :utf8}
          information_schema/columns {table_catalog :utf8, table_schema :utf8, table_name :utf8, column_name :utf8, data_type :utf8,
                                       udt_schema :utf8, udt_name :utf8,
                                       ordinal_position :i32, column_default [:? :utf8], is_nullable :utf8, is_identity :utf8}
          information_schema/schemata {catalog_name :utf8, schema_name :utf8, schema_owner :utf8}}
        (update-vals map->vec-types)))

  (def ^:private pg-catalog-tables
    (-> '{pg_catalog/pg_tables {schemaname :utf8, tablename :utf8, tableowner :utf8, tablespace :null}
          pg_catalog/pg_type {oid :i32, typname :utf8, typnamespace :i32, typowner :i32
                              typcategory :utf8, typtype :utf8, typbasetype :i32, typnotnull :bool, typtypmod :i32
                              typsend :utf8, typreceive :utf8, typinput :utf8, typoutput :utf8
                              typrelid :i32, typelem :i32, typarray :i32}
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
          pg_catalog/pg_am {oid :i32, amname :utf8, amhandler :utf8, amtype :utf8}}
        (update-vals map->vec-types)))

  (def ^:private pg-catalog-template-tables
    (-> '{pg_catalog/pg_user {username :utf8 usesuper :bool passwd [:? :utf8]}}
        (update-vals map->vec-types)))

  (def ^:private xt-derived-tables
    (-> '{xt/trie_stats {schema_name :utf8, table_name :utf8, trie_key :utf8, level :i32, recency [:? :date :day],
                         trie_state :utf8, data_file_size :i64, row_count [:? :i64], temporal_metadata [:? :struct]}

          xt/live_tables {schema_name :utf8, table_name :utf8, row_count :i64}

          xt/live_columns {schema_name :utf8, table_name :utf8, col_name :utf8, col_type :utf8}

          xt/metrics_timers {name :utf8, tags :struct
                             count :i64,
                             mean_time [:? :duration :nano],
                             p75_time [:? :duration :nano]
                             p95_time [:? :duration :nano]
                             p99_time [:? :duration :nano]
                             p999_time [:? :duration :nano]
                             max_time [:? :duration :nano]}

          xt/metrics_gauges {name :utf8, tags :struct, value :f64}
          xt/metrics_counters {name :utf8, tags :struct, count :f64}}
        (update-vals map->vec-types)))

  (def derived-tables
    (merge info-tables pg-catalog-tables xt-derived-tables))

  (defn derived-table [table-ref]
    (get derived-tables (table/ref->schema+table table-ref)))

  (def template-tables
    pg-catalog-template-tables)

  (defn template-table [table-ref]
    (get template-tables (table/ref->schema+table table-ref)))

  (def ^:private ^Cache table-info-cache
    (-> (Caffeine/newBuilder)
        (.maximumSize 1024)
        (.build)))

  (defn table-info [default-db]
    (.get table-info-cache default-db
          (fn [db-name]
            (->> (for [[table fields] (merge derived-tables template-tables)]
                   [(table/->ref db-name table) (set (keys fields))])
                 (into {})))))

  (def ^:private ^Cache table-chains-cache
    (-> (Caffeine/newBuilder)
        (.maximumSize 1024)
        (.build)))

  (defn table-chains [default-db]
    (.get table-chains-cache default-db
          (fn [db-name]
            (->> (keys (table-info db-name))
                 (into [] (mapcat (fn [^TableRef table]
                                    (let [db-name (symbol (.getDbName table))
                                          schema-name (symbol (.getSchemaName table))
                                          table-name (symbol (.getTableName table))]
                                      (cond-> [[[db-name schema-name table-name] table]
                                               [[schema-name table-name] table]]
                                        (= 'pg_catalog schema-name) (conj [[table-name] table]))))))))))

  (def meta-table-schemas
    (merge info-tables pg-catalog-tables pg-catalog-template-tables))

  (def ^:private views
    (-> (set (keys meta-table-schemas))
        (disj 'pg_catalog/pg_user))))

(do
  (def internal-schemas
    #{"xt" "pg_catalog" "information_schema"})

  (def pg-namespaces
    (->> (conj internal-schemas "public")
         (mapv (fn [schema-name]
                 (-> {:schema-name schema-name}
                     (assoc :nspowner (name->oid "xtdb"))
                     (assoc :oid (name->oid schema-name)))))))

  (defn schemas [db-name]
    ;; TODO add other user schemas
    (->> (conj internal-schemas "public")
         (mapv (fn [schema-name]
                 {:catalog-name db-name
                  :schema-name schema-name
                  :schema-owner "xtdb"})))))

(defn tables [schema-info]
  (for [^TableRef table (keys schema-info)]
    {:table-catalog (.getDbName table)
     :table-name (.getTableName table)
     :table-schema (.getSchemaName table)
     :table-type (if (views (table/ref->schema+table table)) "VIEW" "BASE TABLE")}))

(defn pg-tables [schema-info]
  (for [^TableRef table (keys schema-info)
        :let [schema-name (.getSchemaName table)]]
    {:schemaname schema-name
     :tablename (.getTableName table)
     :tableowner "xtdb"
     :tablespace nil}))

(defn pg-class [schema-info]
  (for [^TableRef table (keys schema-info)]
    {:oid (name->oid (table/ref->schema+table table))
     :relname (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) (.getTableName table))
     :relnamespace (name->oid (.getSchemaName table))
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
  (for [^PgType typ PgType/values]
    {:oid (int (.getOid typ))
     :typname (.getTypname typ)
     :typnamespace (name->oid "pg_catalog")
     :typowner (name->oid "xtdb")
     :typtype "b"
     :typcategory (some-> (.getTypcategory typ) .getChr str)
     :typbasetype (int 0)
     :typnotnull false
     :typtypmod (int -1)
     :typrelid (int 0) ; zero for non composite types (or pg_class ref for composite types)
     :typelem (int (or (.getTypelem typ) 0))
     :typarray (int (or (.getTyparray typ) 0))
     :typsend (.getTypsend typ)
     :typreceive (.getTypreceive typ)
     :typinput (or (.getTypinput typ) "")
     :typoutput (or (.getTypoutput typ) "")}))

(def pg-ranges
  {:tstz-range
   {:rngtypid 3910
    :rngsubtype 1184
    :rngmultitypid 4534
    :rngcollation 0
    :rngsubopc 3127
    :rngcanonical ""
    :rngsubdiff "tstzrange_subdiff"}})

(defn pg-range []
  (for [{:keys [rngtypid rngsubtype rngmultitypid rngcollation rngsubopc rngcanonical rngsubdiff]} (vals pg-ranges)]
    {:rngtypid (int rngtypid)
     :rngsubtype (int rngsubtype)
     :rngmultitypid (int rngmultitypid)
     :rngcollation (int rngcollation)
     :rngsubopc (int rngsubopc)
     :rngcanonical rngcanonical
     :rngsubdiff rngsubdiff}))

(defn columns [col-rows]
  (for [{:keys [^TableRef table, ^VectorType vec-type, idx], col-name :name} col-rows
        :let [typname (.getTypname (PgType/fromVectorType vec-type))]]
    {:table-catalog (.getDbName table)
     :table-name (.getTableName table)
     :table-schema (.getSchemaName table)
     :column-name (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) (str col-name))
     :data-type (pr-str (st/render-type vec-type))
     :udt-schema "pg_catalog"
     :udt-name (or typname "json")
     :ordinal-position (int idx)
     :column-default nil
     :is-nullable (if (types/nullable-vec-type? vec-type) "YES" "NO")
     :is-identity "NO"}))

(defn pg-attribute [col-rows]
  (for [{:keys [idx table name ^VectorType vec-type]} col-rows
        :let [^PgType pg-type (PgType/fromVectorType vec-type)
              ^PgType pg-type (if (or (nil? pg-type) (identical? pg-type PgType/PG_DEFAULT)) PgType/PG_JSON pg-type)]]
    {:attrelid (name->oid (table/ref->schema+table table))
     :attname (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) (str name))
     :atttypid (.getOid pg-type)
     :attlen (int (or (.getTyplen pg-type) -1))
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

(def pg-ams
  [{:oid 2, :amname "heap", :amhandler "heap_tableam_handler", :amtype "t"}
   {:oid 403, :amname "btree", :amhandler "bthandler", :amtype "i"}
   {:oid 405, :amname "hash", :amhandler "hashhandler", :amtype "i"}])

(defn pg-am []
  (for [{:keys [oid amname amhandler amtype]} pg-ams]
    {:oid (int oid), :amname amname, :amhandler amhandler, :amtype amtype}))

(do ; to eval them both
  (def procs
    {'pg_catalog/array_in {:oid (name->oid "array_in")
                           :proname "array_in"
                           :pronamespace (name->oid "pg_catalog")}})

  (def oid->proc
    (into {} (map (juxt (comp :oid val) key) procs))))

(defn pg-proc []
  (for [{:keys [oid proname pronamespace]} (vals procs)]
    {:oid oid, :proname proname, :pronamespace pronamespace}))

(defn pg-database [db-name]
  ;; TODO get all databases here
  ;; currently not available due to cyclic dep
  [{:oid (name->oid db-name)
    :datname db-name
    :datallowconn true
    :datistemplate false}])

(defn pg-stat-user-tables [schema-info]
  (for [^TableRef table (keys schema-info)]
    {:relid (name->oid (table/ref->schema+table table))
     :relname (.getTableName table)
     :schemaname (.getSchemaName table)
     :n-live-tup 0}))

(defn pg-settings []
  (for [[setting-name setting] {"max_index_keys" "32"}]
    {:name setting-name
     :setting setting}))

(def ^:private pg-user-field
  (types/->field "put" [:struct {"_id" :utf8
                                 "username" :utf8
                                 "usesuper" :bool
                                 "passwd" [:? :utf8]}]))

(def ^:private initial-user-data
  [{:_id "xtdb", :username "xtdb", :usesuper true, :passwd (authn.crypt/encrypt-pw "xtdb")}])

(defn pg-user-template-page+trie [allocator]
  (util/with-close-on-catch [out-rel (Trie/openLogDataWriter allocator (Trie/dataRelSchema pg-user-field))]
    (let [{^VectorWriter iid-vec "_iid",
           ^VectorWriter sf-vec "_system_from"
           ^VectorWriter vf-vec "_valid_from",
           ^VectorWriter vt-vec "_valid_to"} out-rel

          ^VectorWriter put-vec (get-in out-rel ["op" "put"])]

      (doseq [user initial-user-data]
        (.writeObject iid-vec (util/->iid (:_id user)))
        (.writeLong sf-vec 0)
        (.writeLong vf-vec 0)
        (.writeLong vt-vec Long/MAX_VALUE)
        (.writeObject put-vec user)
        (.endRow out-rel))
      (let [dummy-trie (reduce #(.plus ^MemoryHashTrie %1 %2)
                               (trie/->live-trie 32 1024 iid-vec)
                               (range (count initial-user-data)))]
        [out-rel dummy-trie]))))

(defn trie-stats [^TrieCatalog trie-catalog]
  (for [^TableRef table (.getTables trie-catalog)
        :let [trie-state (trie-cat/trie-state trie-catalog table)]
        {:keys [trie-key level recency state data-file-size trie-metadata]} (trie-cat/all-tries trie-state)
        :let [{:keys [row-count] :as trie-meta} (some-> trie-metadata trie-cat/<-trie-metadata)]]
    {:schema-name (.getSchemaName table), :table-name (.getTableName table)
     :trie-key trie-key, :level (int level), :recency recency, :data-file-size data-file-size
     :trie-state (name state), :row-count row-count, :temporal-metadata (some-> trie-meta (dissoc :row-count :iid-bloom))}))

(defn live-tables [^Snapshot snap]
  (let [li-snap (.getLiveIndex snap)]
    (for [^TableRef table (.getLiveTables li-snap)
          :let [live-table (.liveTable li-snap table)]]
      {:schema-name (.getSchemaName table)
       :table-name (.getTableName table)
       :row-count (long (.getRowCount (.getLiveRelation live-table)))})))

(defn live-columns [^Snapshot snap]
  (let [li-snap (.getLiveIndex snap)]
    (for [^TableRef table (.getLiveTables li-snap)
          [col-name col-type] (.getTypes (.liveTable li-snap table))]
      {:schema-name (.getSchemaName table)
       :table-name (.getTableName table)
       :col-name col-name
       :col-type (pr-str (st/render-type col-type))})))

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

(deftype InformationSchemaCursor [^:unsynchronized-mutable ^RelationReader out-rel-view,
                                  ^:unsynchronized-mutable out-rel]
  ICursor
  (getCursorType [_] "information-schema")
  (getChildCursors [_] [])

  (tryAdvance [this c]
    (boolean
     (when-let [^RelationReader out-rel-view (.out-rel-view this)]
       (try
         (set! (.out-rel-view this) nil)
         (set! (.out-rel this) nil)
         (.accept c out-rel-view)
         true
         (finally
           (util/close out-rel))))))

  (close [_]
    (some-> out-rel util/close)))

(defprotocol InfoSchema
  (->cursor [info-schema allocator db snapshot derived-table-schema
             table col-names col-preds
             schema params])

  (table-template [info-schema table-ref]))

(defmethod ig/expand-key :xtdb/information-schema [k opts]
  {k (into {:allocator (ig/ref :xtdb/allocator)
            :metrics-registry (ig/ref :xtdb.metrics/registry)}
           opts)})

(defmethod ig/init-key :xtdb/information-schema [_ {:keys [allocator metrics-registry]}]
  (let [pg-user (pg-user-template-page+trie allocator)]
    (reify InfoSchema
      (table-template [_ table-ref]
        (when (= 'pg_catalog/pg_user (table/ref->schema+table table-ref))
          pg-user))

      (->cursor [_ allocator db snap derived-table-schema
                 table col-names col-preds
                 schema params]
        ;; TODO should use the schema passed to it, but also regular merge is insufficient here for colFields
        ;; should be types/merge-types as per scan-vec-types
        (let [db-state (.getQueryState db)
              db-name (.getName db-state)
              table-catalog (.getTableCatalog db-state)
              trie-catalog (.getTrieCatalog db-state)
              schema-info (-> (merge-with merge
                                          (.getTypes table-catalog)
                                          (some-> (.getLiveIndex ^Snapshot snap)
                                                  (.getAllColumnTypes)))
                              (merge meta-table-schemas)
                              (update-keys (fn [k]
                                             (cond
                                               (instance? TableRef k) k
                                               (symbol? k) (table/->ref db-name k)))))]

          (util/with-close-on-catch [out-rel (Relation. ^BufferAllocator allocator
                                                        ^Map (update-keys derived-table-schema str))]

            (.writeRows out-rel (->> (case (table/ref->schema+table table)
                                       information_schema/tables (tables schema-info)
                                       information_schema/columns (columns (schema-info->col-rows schema-info))
                                       information_schema/schemata (schemas db-name)
                                       pg_catalog/pg_tables (pg-tables schema-info)
                                       pg_catalog/pg_type (pg-type)
                                       pg_catalog/pg_class (pg-class schema-info)
                                       pg_catalog/pg_description nil
                                       pg_catalog/pg_views nil
                                       pg_catalog/pg_matviews nil
                                       pg_catalog/pg_attribute (pg-attribute (schema-info->col-rows schema-info))
                                       pg_catalog/pg_namespace (pg-namespace)
                                       pg_catalog/pg_proc (pg-proc)
                                       pg_catalog/pg_database (pg-database db-name)
                                       pg_catalog/pg_stat_user_tables (pg-stat-user-tables schema-info)
                                       pg_catalog/pg_settings (pg-settings)
                                       pg_catalog/pg_range (pg-range)
                                       pg_catalog/pg_am (pg-am)
                                       xt/trie_stats (trie-stats trie-catalog)
                                       xt/live_tables (live-tables snap)
                                       xt/live_columns (live-columns snap)
                                       xt/metrics_timers (metrics-timers metrics-registry)
                                       xt/metrics_gauges (metrics-gauges metrics-registry)
                                       xt/metrics_counters (metrics-counters metrics-registry)
                                       (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table))))
                                     (into-array java.util.Map)))

            ;;TODO reuse relation selector code from trie cursor
            (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                                (.select rel (.select col-pred allocator rel schema params)))
                                              (-> out-rel
                                                  (->> (filter (comp (set col-names) #(.getName ^VectorReader %))))
                                                  (vr/rel-reader (.getRowCount out-rel))
                                                  (vr/with-absent-cols allocator col-names))
                                              (vals col-preds))
                                      out-rel))))

      AutoCloseable
      (close [_]
        (util/close (first pg-user))))))

(defmethod ig/halt-key! :xtdb/information-schema [_ info-schema]
  (util/close info-schema))
