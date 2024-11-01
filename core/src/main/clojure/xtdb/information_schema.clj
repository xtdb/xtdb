(ns xtdb.information-schema
  (:require xtdb.metadata
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Schema)
           (xtdb ICursor)
           xtdb.api.query.IKeyFn
           (xtdb.metadata IMetadataManager)
           xtdb.operator.SelectionSpec
           (xtdb.vector IVectorReader RelationReader)
           (xtdb.watermark Watermark)))

(defn name->oid [s]
  (Math/abs ^Integer (hash s)))

;;TODO add temporal cols
(defn schema-info->col-rows [schema-info]
  (for [table-entry schema-info
        [idx col] (map-indexed #(vector %1 %2) (val table-entry))
        :let [table (key table-entry)
              name (key col)
              col-field (val col)]]

    {:idx (inc idx) ;; no guarentee of order/stability of idx for a given col
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
      pg_catalog/pg_class {oid :i32, relname :utf8, relnamespace :i32, relkind :utf8}
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
      pg_catalog/pg_user {username :utf8 usesuper :bool passwd [:union #{:utf8 :null}]}})

  (def derived-tables
    (-> (merge info-tables pg-catalog-tables)
        (update-vals (fn [col-types]
                       (->> (for [[col-name col-type] col-types]
                              [col-name (types/col-type->field col-name col-type)])
                            (into {}))))))

  (def table-info (-> derived-tables (update-vals (comp set keys))))

  (def unq-pg-catalog
    (-> pg-catalog-tables
        (update-vals keys)
        (update-keys (comp symbol name)))))

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
     :table-type "BASE TABLE"}))

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
     :relnamespace (name->oid "public")
     :relkind "r"}))

(defn pg-type []
  (for [{:keys [oid typname typsend typreceive typelem typinput typoutput]} (->> (vals types/pg-types)
                                                                                 (remove (comp zero? :oid)))]
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
  (for [{:keys [rngtypid rngsubtype rngmultitypid rngcollation rngsubopc rngcanonical rngsubdiff]} (vals types/pg-ranges)]
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
        :let [{:keys [column-oid typlen]} (types/field->pg-type (types/field-with-name field name))]]
    {:attrelid (name->oid table)
     :attname (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) name)
     :atttypid (int column-oid)
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

(defn pg-user []
  [{:username "xtdb" :usesuper true :passwd "xtdb"}
   {:username "anonymous" :usesuper false :passwd nil}])

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

(defn ->cursor [allocator derived-table-schema table col-names col-preds schema params ^IMetadataManager metadata-mgr ^Watermark wm]
  (util/with-close-on-catch [root (VectorSchemaRoot/create (Schema. (vec (vals derived-table-schema)))
                                                           allocator)]
    ;;TODO should use the schema passed to it, but also regular merge is insufficient here for colFields
    ;;should be types/merge-fields as per scan-fields
    (let [schema-info (-> (merge-with merge
                                      (.allColumnFields metadata-mgr)
                                      (some-> (.liveIndex wm)
                                              (.allColumnFields)))
                          (update-keys symbol))

          out-rel-wtr (vw/root->writer root)
          out-rel (vw/rel-wtr->rdr (doto out-rel-wtr
                                     (.writeRows (case table
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
                                                   pg_catalog/pg_user (pg-user)
                                                   (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table)))))
                                     (.syncRowCount)))]

      ;;TODO reuse relation selector code from trie cursor
      (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                          (.select rel (.select col-pred allocator rel schema params)))
                                        (-> out-rel
                                            (->> (filter (comp (set col-names) #(.getName ^IVectorReader %))))
                                            (vr/rel-reader (.rowCount out-rel))
                                            (vr/with-absent-cols allocator col-names))
                                        (vals col-preds))
                                root))))
