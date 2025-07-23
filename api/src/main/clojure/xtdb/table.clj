(ns ^:no-doc xtdb.table
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cognitect.transit :as transit])
  (:import [xtdb.table TableRef]
           xtdb.util.NormalForm))

(defmethod print-method TableRef [^TableRef ref, ^java.io.Writer w]
  (let [db-name (symbol (.getDbName ref))
        schema (.getSchemaName ref)
        schema+table (if (= schema "public")
                       (symbol (.getTableName ref))
                       (symbol schema (.getTableName ref)))]
    (.write w (format "#xt/table %s"
                      (pr-str (if (= db-name 'xtdb)
                                schema+table
                                [db-name schema+table]))))))

(defmethod print-dup TableRef [ref w] (print-method ref w))
(defmethod pp/simple-dispatch TableRef [it] (print-method it *out*))

(s/def ::ref #(instance? TableRef %))

(defn ->ref
  (^xtdb.table.TableRef [ref-form]
   (if (vector? ref-form)
     (->ref (first ref-form) (second ref-form))
     (->ref "xtdb" ref-form)))

  (^xtdb.table.TableRef [db-name schema+table]
   (cond
     (string? schema+table) (let [[table schema] (reverse (str/split schema+table #"/" 2))]
                              (->ref db-name schema table))
     (simple-symbol? schema+table) (->ref db-name nil schema+table)
     (qualified-symbol? schema+table) (->ref db-name (namespace schema+table) (name schema+table))
     (keyword? schema+table) (recur db-name (symbol (NormalForm/normalTableName schema+table)))))

  (^xtdb.table.TableRef [db-name schema table]
   (TableRef. (some-> db-name str) (or (some-> schema str) "public") (str table))))

(defn ref->sym [^TableRef table-ref]
  ;; only a test util - although it's currently used in pg and info-schema
  (assert (= "xtdb" (.getDbName table-ref)) "TableRef must be from the xtdb database")
  (symbol (.getSchemaName table-ref) (.getTableName table-ref)))

(def transit-read-handlers
  {"xt/table" (transit/read-handler (fn [{:keys [db-name schema-name table-name]}]
                                      (->ref db-name schema-name table-name)))})

(def transit-write-handlers
  {TableRef (transit/write-handler "xt/table"
                                   (fn [^TableRef table]
                                     {:db-name (.getDbName table)
                                      :schema-name (.getSchemaName table)
                                      :table-name (.getTableName table)}))})
