(ns xtdb.information-schema
  (:require xtdb.metadata
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import
   (org.apache.arrow.vector VectorSchemaRoot)
   (org.apache.arrow.vector.types.pojo Schema Field)
   xtdb.operator.IRelationSelector
   (xtdb.vector RelationReader IVectorWriter IRelationWriter)
   (xtdb ICursor)
   (xtdb.metadata IMetadataManager)
   (xtdb.watermark Watermark)))

;;TODO add temporal cols

(defn schema-info->col-rows [schema-info]
  (for [table-entry schema-info
        [idx col] (map-indexed #(vector %1 %2) (val table-entry))
        :let [table (key table-entry)
              name (key col)
              col-field (val col)]]

    {:idx idx ;; no guarentee of order/stability of idx for a given col
     :table table
     :name name
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
   'pg_catalog/pg_views {"schemaname" (types/col-type->field "schemaname" :utf8)
                         "viewname" (types/col-type->field "viewname" :utf8)
                         "viewowner" (types/col-type->field "viewowner" :utf8)}
   'pg_catalog/pg_matviews {"schemaname" (types/col-type->field "schemaname" :utf8)
                            "matviewname" (types/col-type->field "matviewname" :utf8)
                            "matviewowner" (types/col-type->field "matviewowner" :utf8)}
   'pg_catalog/pg_attribute {"attrelid" (types/col-type->field "attrelid" :i32)
                             "attname" (types/col-type->field "attname" :utf8)
                             "atttypid" (types/col-type->field "atttypid" :i32)
                             "attlen" (types/col-type->field "attlen" :i16)
                             "attnum" (types/col-type->field "attnum" :i16)}
   'pg_catalog/pg_namespace {"oid" (types/col-type->field "oid" :i32)
                             "nspname" (types/col-type->field "nspname" :utf8)
                             "nspowner" (types/col-type->field "nspowner" :i32)
                             "nspacl" (types/col-type->field "nspacl" :null)}})

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

(def pg-namespaces (map (fn [{:strs [schema_owner schema_name] :as schema}]
                          (-> schema
                              (assoc "nspowner" (hash schema_owner))
                              (assoc "oid" (hash schema_name))))
                        schemas))

(defn tables [^IRelationWriter rel-wtr schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "table_catalog" (.writeObject col-wtr "xtdb")
        "table_name" (.writeObject col-wtr table)
        "table_schema" (.writeObject col-wtr "public")
        "table_type" (.writeObject col-wtr "BASE TABLE")))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-tables [^IRelationWriter rel-wtr schema-info]
  (doseq [table (keys schema-info)]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "schemaname" (.writeObject col-wtr "public")
        "tablename" (.writeObject col-wtr table)
        "tableowner" (.writeObject col-wtr "xtdb")
        "tablespace" (.writeObject col-wtr nil)))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn columns [^IRelationWriter rel-wtr col-rows]
  (doseq [{:keys [table name type]} col-rows]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "table_catalog" (.writeObject col-wtr "xtdb")
        "table_name" (.writeObject col-wtr table)
        "table_schema" (.writeObject col-wtr "public")
        "column_name" (.writeObject col-wtr name)
        "data_type" (.writeObject col-wtr (pr-str type))))
    (.endRow rel-wtr))
  (.syncRowCount rel-wtr)
  rel-wtr)

(defn pg-attribute [^IRelationWriter rel-wtr col-rows]
  (doseq [{:keys [idx table name _type]} col-rows]
    (.startRow rel-wtr)
    (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
      (case col
        "attrelid" (.writeInt col-wtr (hash table))
        "attname" (.writeObject col-wtr name)
        "atttypid" (.writeInt col-wtr 114) ;; = json - avoiding circular dep on pgwire.clj
        "attlen" (.writeShort col-wtr -1)
        "attnum" (.writeShort col-wtr idx)))
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

(defn ->cursor [allocator derived-table-schema table col-names col-preds params ^IMetadataManager metadata-mgr ^Watermark wm]
  (util/with-close-on-catch [root (VectorSchemaRoot/create (Schema. (or (vals (select-keys derived-table-schema col-names)) [])) allocator)]
    (let [schema-info (merge-with merge
                                  (.allColumnFields metadata-mgr)
                                  (some-> (.liveIndex wm)
                                          (.allColumnFields)))
          out-rel-wtr (vw/root->writer root)
          out-rel (vw/rel-wtr->rdr (case table
                                     information_schema/tables (tables out-rel-wtr schema-info)
                                     information_schema/columns (columns out-rel-wtr (schema-info->col-rows schema-info))
                                     information_schema/schemata (schemata out-rel-wtr)
                                     pg_catalog/pg_tables (pg-tables out-rel-wtr schema-info)
                                     pg_catalog/pg_views out-rel-wtr
                                     pg_catalog/pg_matviews out-rel-wtr
                                     pg_catalog/pg_attribute (pg-attribute out-rel-wtr (schema-info->col-rows schema-info))
                                     pg_catalog/pg_namespace (pg-namespace out-rel-wtr)
                                     (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table)))))]

      ;;TODO reuse relation selector code from tri cursor
      (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^IRelationSelector col-pred]
                                          (.select rel (.select col-pred allocator rel params)))
                                        (-> out-rel
                                            (vr/with-absent-cols allocator col-names))
                                        (vals col-preds)) root))))
