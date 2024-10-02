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
           (xtdb.vector IRelationWriter IVectorWriter RelationReader)
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

(def info-tables
  {'information_schema/tables {"table_catalog" (types/col-type->field "table_catalog" :utf8)
                               "table_schema" (types/col-type->field "table_schema" :utf8)
                               "table_name" (types/col-type->field "table_name" :utf8)
                               "table_type" (types/col-type->field "table_type" :utf8)}
   'information_schema/columns {"table_catalog" (types/col-type->field "table_catalog" :utf8)
                                "table_schema" (types/col-type->field "table_schema" :utf8)
                                "table_name" (types/col-type->field "table_name" :utf8)
                                "column_name" (types/col-type->field "column_name" :utf8)
                                "data_type" (types/col-type->field "data_type" :utf8)}
   'information_schema/schemata {"catalog_name" (types/col-type->field "catalog_name" :utf8)
                                 "schema_name" (types/col-type->field "schema_name" :utf8)
                                 "schema_owner" (types/col-type->field "schema_owner" :utf8)}})

(def pg-catalog-tables
  {'pg_catalog/pg_tables {"schemaname" (types/col-type->field "schemaname" :utf8)
                          "tablename" (types/col-type->field "tablename" :utf8)
                          "tableowner" (types/col-type->field "tableowner" :utf8)
                          "tablespace" (types/col-type->field "tablespace" :null)}
   'pg_catalog/pg_type {"oid" (types/col-type->field "oid" :i32)
                        "typname" (types/col-type->field "typname" :utf8)
                        "typnamespace" (types/col-type->field "typnamespace" :i32)
                        "typowner" (types/col-type->field "typowner" :i32)
                        "typtype" (types/col-type->field "typtype" :utf8)
                        "typbasetype" (types/col-type->field "typbasetype" :i32)
                        "typnotnull" (types/col-type->field "typnotnull" :bool)
                        "typtypmod" (types/col-type->field "typtypmod" :i32)
                        "typsend" (types/col-type->field "typsend" :utf8)
                        "typreceive" (types/col-type->field "typreceive" :utf8)}
   'pg_catalog/pg_class {"oid" (types/col-type->field "oid" :i32)
                         "relname" (types/col-type->field "relname" :utf8)
                         "relnamespace" (types/col-type->field "relnamespace" :i32)
                         "relkind" (types/col-type->field "relkind" :utf8)}
   'pg_catalog/pg_description {"objoid" (types/col-type->field "objoid" :i32)
                               "classoid" (types/col-type->field "classoid" :i32)
                               "objsubid" (types/col-type->field "objsubid" :i16)
                               "description"(types/col-type->field "description" :utf8)}
   'pg_catalog/pg_views {"schemaname" (types/col-type->field "schemaname" :utf8)
                         "viewname" (types/col-type->field "viewname" :utf8)
                         "viewowner" (types/col-type->field "viewowner" :utf8)}
   'pg_catalog/pg_matviews {"schemaname" (types/col-type->field "schemaname" :utf8)
                            "matviewname" (types/col-type->field "matviewname" :utf8)
                            "matviewowner" (types/col-type->field "matviewowner" :utf8)}
   'pg_catalog/pg_attribute {"attrelid" (types/col-type->field "attrelid" :i32)
                             "attname" (types/col-type->field "attname" :utf8)
                             "atttypid" (types/col-type->field "atttypid" :i32)
                             "attlen" (types/col-type->field "attlen" :i32)
                             "attnum" (types/col-type->field "attnum" :i32)
                             "attisdropped" (types/col-type->field "attisdropped" :bool)
                             "attnotnull" (types/col-type->field "attnotnull" :bool)
                             "atttypmod" (types/col-type->field "atttypmod" :i32)
                             "attidentity" (types/col-type->field "attidentity" :utf8)
                             "attgenerated"(types/col-type->field "attgenerated" :utf8)}
   'pg_catalog/pg_namespace {"oid" (types/col-type->field "oid" :i32)
                             "nspname" (types/col-type->field "nspname" :utf8)
                             "nspowner" (types/col-type->field "nspowner" :i32)
                             "nspacl" (types/col-type->field "nspacl" :null)}
   'pg_catalog/pg_proc {"oid" (types/col-type->field "oid" :i32)
                        "proname" (types/col-type->field "proname" :utf8)
                        "pronamespace" (types/col-type->field "pronamespace" :i32)}
   'pg_catalog/pg_database {"oid" (types/col-type->field "oid" :i32)
                            "datname" (types/col-type->field "datname" :utf8)
                            "datallowconn" (types/col-type->field "datallowconn" :bool)
                            "datistemplate" (types/col-type->field "datistemplate" :bool)}

   'pg_catalog/pg_stat_user_tables {"relid" (types/col-type->field "relid" :i32)
                                    "schemaname" (types/col-type->field "schemaname" :utf8)
                                    "relname" (types/col-type->field "relname" :utf8)
                                    "n_live_tup" (types/col-type->field "n_live_tup" :i64)}

   'pg_catalog/pg_settings {"name" (types/col-type->field "name" :utf8)
                            "setting" (types/col-type->field "setting" :utf8)}})

(def derived-tables (merge info-tables pg-catalog-tables))
(def table-info (-> derived-tables (update-vals (comp set keys))))

(def unq-pg-catalog
  (-> pg-catalog-tables
      (update-vals (comp #(into #{} (map symbol) %) keys))
      (update-keys (comp symbol name))))

(def schemas
  [{"catalog_name" "xtdb"
    "schema_name" "pg_catalog"
    "schema_owner" "xtdb"}
   {"catalog_name" "xtdb"
    "schema_name" "public"
    "schema_owner" "xtdb"}
   {"catalog_name" "xtdb"
    "schema_name" "information_schema"
    "schema_owner" "xtdb"}])

(def pg-namespaces
  (mapv (fn [{:strs [schema_owner schema_name] :as schema}]
          (-> schema
              (assoc "nspowner" (name->oid schema_owner))
              (assoc "oid" (name->oid schema_name))))
        schemas))

(defn tables [^IRelationWriter rel-wtr schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "table_catalog" (.writeObject col-wtr "xtdb")
        "table_name" (.writeObject col-wtr (name table))
        "table_schema" (.writeObject col-wtr (namespace table))
        "table_type" (.writeObject col-wtr "BASE TABLE")))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-tables [^IRelationWriter rel-wtr schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "schemaname" (.writeObject col-wtr (namespace table))
        "tablename" (.writeObject col-wtr (name table))
        "tableowner" (.writeObject col-wtr "xtdb")
        "tablespace" (.writeObject col-wtr nil)))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-class [^IRelationWriter rel-wtr schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "oid" (.writeInt col-wtr (name->oid table))
        "relname" (.writeObject col-wtr (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) (name table)))
        "relnamespace" (.writeObject col-wtr (name->oid "public"))
        "relkind" (.writeObject col-wtr "r")))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-type [^IRelationWriter rel-wtr]
  (doseq [{:keys [oid typname typsend typreceive] :as x} (remove #(= 0 (:oid %)) (vals types/pg-types))]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "oid" (.writeInt col-wtr oid)
        "typname" (.writeObject col-wtr typname)
        "typnamespace" (.writeObject col-wtr (name->oid "pg_catalog"))
        "typowner" (.writeInt col-wtr (name->oid "xtdb"))
        "typtype" (.writeObject col-wtr "b")
        "typbasetype" (.writeInt col-wtr 0)
        "typnotnull" (.writeBoolean col-wtr false)
        "typtypmod" (.writeInt col-wtr -1)
        "typsend" (.writeObject col-wtr typsend)
        "typreceive" (.writeObject col-wtr typreceive)))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn columns [^IRelationWriter rel-wtr col-rows]
  (doseq [{:keys [table type], col-name :name} col-rows]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "table_catalog" (.writeObject col-wtr "xtdb")
        "table_name" (.writeObject col-wtr (name table))
        "table_schema" (.writeObject col-wtr (namespace table))
        "column_name" (.writeObject col-wtr (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) col-name))
        "data_type" (.writeObject col-wtr (pr-str type))))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-attribute [^IRelationWriter rel-wtr col-rows]
  (doseq [{:keys [idx table name field]} col-rows
          :let [{:keys [column-oid typlen]} (types/field->pg-type (types/field-with-name field name))]]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "attrelid" (.writeInt col-wtr (name->oid table))
        "attname" (.writeObject col-wtr (.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) name))
        "atttypid" (.writeInt col-wtr column-oid)
        "attlen" (.writeInt col-wtr (or typlen -1))
        "attnum" (.writeInt col-wtr idx)
        "attisdropped" (.writeBoolean col-wtr false)
        "attnotnull" (.writeBoolean col-wtr false)
        "atttypmod" (.writeInt col-wtr -1)
        "attidentity" (.writeObject col-wtr "")
        "attgenerated" (.writeObject col-wtr "")))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn schemata [^IRelationWriter rel-wtr]
  (doseq [{:strs [catalog_name schema_name schema_owner]} schemas]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "catalog_name" (.writeObject col-wtr catalog_name)
        "schema_name" (.writeObject col-wtr schema_name)
        "schema_owner" (.writeObject col-wtr schema_owner)))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-namespace [^IRelationWriter rel-wtr]
  (doseq [{:strs [oid schema_name nspowner]} pg-namespaces]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "oid" (.writeInt col-wtr oid)
        "nspname" (.writeObject col-wtr schema_name)
        "nspowner" (.writeInt col-wtr nspowner)
        "nspacl" (.writeObject col-wtr nil)))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(do ; to eval them both
  (def procs
    {'pg_catalog/array_in {:oid (name->oid "array_in")
                           :proname "array_in"
                           :pronamespace (name->oid "pg_catalog")}})

  (def oid->proc
    (into {} (map (juxt (comp :oid val) key) procs))))

(defn pg-proc [^IRelationWriter rel-wtr]
  (doseq [{:keys [oid proname pronamespace]} (vals procs)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "oid" (.writeInt col-wtr oid)
        "proname" (.writeObject col-wtr proname)
        "pronamespace" (.writeInt col-wtr pronamespace)))
    (.endRow rel-wtr))

  rel-wtr)

(def databases
  [{:datname "xtdb"}])

(defn pg-database [^IRelationWriter rel-wtr]
  (doseq [{:keys [datname]} databases]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "oid" (.writeInt col-wtr (name->oid datname))
        "datname" (.writeObject col-wtr datname)
        "datallowconn" (.writeBoolean col-wtr true)
        "datistemplate" (.writeBoolean col-wtr false)))
    (.endRow rel-wtr))

  rel-wtr)

(defn pg-stat-user-tables [^IRelationWriter rel-wtr, schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "relid" (.writeInt col-wtr (name->oid (name table)))
        "relname" (.writeObject col-wtr (name table))
        "schemaname" (.writeObject col-wtr (namespace table))
        "n_live_tup" (.writeLong col-wtr 0)))
    (.endRow rel-wtr))

  rel-wtr)

(defn pg-settings [^IRelationWriter rel-wtr]
  (doseq [[setting-name setting] {"max_index_keys" "32"}]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "name" (.writeObject col-wtr setting-name)
        "setting" (.writeObject col-wtr setting)))
    (.endRow rel-wtr))

  rel-wtr)

(deftype InformationSchemaCursor [^:unsynchronized-mutable ^RelationReader out-rel vsr]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-let [out-rel out-rel]
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
  (util/with-close-on-catch [root (VectorSchemaRoot/create (Schema. (or (vals (select-keys derived-table-schema col-names)) [])) allocator)]
    ;;TODO should use the schema passed to it, but also regular merge is insufficient here for colFields
    ;;should be types/merge-fields as per scan-fields
    (let [schema-info (-> (merge-with merge
                                        (.allColumnFields metadata-mgr)
                                        (some-> (.liveIndex wm)
                                                (.allColumnFields)))
                          (update-keys symbol))

          out-rel-wtr (vw/root->writer root)
          out-rel (vw/rel-wtr->rdr (case table
                                     information_schema/tables (tables out-rel-wtr schema-info)
                                     information_schema/columns (columns out-rel-wtr (schema-info->col-rows schema-info))
                                     information_schema/schemata (schemata out-rel-wtr)
                                     pg_catalog/pg_tables (pg-tables out-rel-wtr schema-info)
                                     pg_catalog/pg_type (pg-type out-rel-wtr)
                                     pg_catalog/pg_class (pg-class out-rel-wtr schema-info)
                                     pg_catalog/pg_description out-rel-wtr
                                     pg_catalog/pg_views out-rel-wtr
                                     pg_catalog/pg_matviews out-rel-wtr
                                     pg_catalog/pg_attribute (pg-attribute out-rel-wtr (schema-info->col-rows schema-info))
                                     pg_catalog/pg_namespace (pg-namespace out-rel-wtr)
                                     pg_catalog/pg_proc (pg-proc out-rel-wtr)
                                     pg_catalog/pg_database (pg-database out-rel-wtr)
                                     pg_catalog/pg_stat_user_tables (pg-stat-user-tables out-rel-wtr schema-info)
                                     pg_catalog/pg_settings (pg-settings out-rel-wtr)
                                     (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table)))))]

      ;;TODO reuse relation selector code from tri cursor
      (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                          (.select rel (.select col-pred allocator rel schema params)))
                                        (-> out-rel
                                            (vr/with-absent-cols allocator col-names))
                                        (vals col-preds)) root))))
