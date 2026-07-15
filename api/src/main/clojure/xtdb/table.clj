(ns ^:no-doc xtdb.table
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cognitect.transit :as transit]
            [xtdb.error :as err])
  (:import clojure.lang.Symbol
           [xtdb.api TableRef]
           xtdb.util.NormalForm))

(defmethod print-method TableRef [^TableRef ref, ^java.io.Writer w]
  (let [schema (.getSchemaName ref)
        schema+table (if (= schema "public")
                       (symbol (.getTableName ref))
                       (symbol schema (.getTableName ref)))]
    (.write w (format "#xt/table %s" (pr-str schema+table)))))

(defmethod print-dup TableRef [ref w] (print-method ref w))
(defmethod pp/simple-dispatch TableRef [it] (print-method it *out*))

(s/def ::ref #(instance? TableRef %))

(defn ->ref
  (^xtdb.api.TableRef [schema+table]
   (cond
     (instance? TableRef schema+table) schema+table
     (string? schema+table) (let [[table schema] (reverse (str/split schema+table #"/" 2))]
                              (->ref schema table))
     (simple-symbol? schema+table) (->ref nil schema+table)
     (qualified-symbol? schema+table) (->ref (namespace schema+table) (name schema+table))
     (keyword? schema+table) (recur (symbol (NormalForm/normalTableName schema+table)))
     :else (throw (err/incorrect ::invalid-table-ref
                                 (format "Expected a table reference (symbol, string, keyword, or TableRef), got: %s"
                                         (pr-str schema+table))))))

  (^xtdb.api.TableRef [schema table]
   (TableRef. (or (some-> schema str) "public") (str table))))

(defn normal-db-name
  "Canonicalises a database name (keyword/symbol/string) to its normal-form string,
   matching the names held in the database catalog."
  [db]
  (cond
    (nil? db) nil
    (keyword? db) (recur (symbol db))
    (symbol? db) (str (NormalForm/normalForm ^Symbol db))
    :else (str db)))

(defn ref->schema+table [table-ref-or-sym]
  (cond
    (symbol? table-ref-or-sym) table-ref-or-sym
    (instance? TableRef table-ref-or-sym) (let [^TableRef table-ref table-ref-or-sym]
                                            (symbol (.getSchemaName table-ref) (.getTableName table-ref)))
    :else (throw (err/fault ::invalid-table-ref
                            (format "Expected a TableRef or a symbol, got: %s" (pr-str table-ref-or-sym))))))

(def transit-read-handlers
  {"xt/table" (transit/read-handler (fn [{:keys [schema-name table-name]}]
                                      (->ref schema-name table-name)))})

(def transit-write-handlers
  {TableRef (transit/write-handler "xt/table"
                                   (fn [^TableRef table]
                                     {:schema-name (.getSchemaName table)
                                      :table-name (.getTableName table)}))})
