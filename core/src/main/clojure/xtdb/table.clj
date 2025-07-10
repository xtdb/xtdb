(ns xtdb.table
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import [xtdb.table TableRef]
           xtdb.util.NormalForm))

(defrecord Ref [schema-name table-name]
  TableRef
  (getSchemaName [_] schema-name)
  (getTableName [_] table-name))

(defmethod print-method Ref [^Ref ref, ^java.io.Writer w]
  (.write w (format "#xt/table %s"
                    (let [schema (.getSchemaName ref)]
                      (if (= schema "public")
                        (symbol (.getTableName ref))
                        (symbol schema (.getTableName ref)))))))

(defmethod pp/simple-dispatch Ref [it] (print-method it *out*))

(s/def ::ref #(instance? TableRef %))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; data-readers.clj
(defn ->ref
  (^xtdb.table.TableRef [schema+table]
   (cond
     (string? schema+table) (let [[table schema] (reverse (str/split schema+table #"/" 2))]
                              (->Ref schema table))
     (simple-symbol? schema+table) (->ref nil schema+table)
     (qualified-symbol? schema+table) (->Ref (namespace schema+table) (name schema+table))
     (keyword? schema+table) (recur (symbol (NormalForm/normalTableName schema+table)))))

  (^xtdb.table.TableRef [schema table]
   (->Ref (or (some-> schema str) "public") (str table))))

(defn ref->sym [^TableRef table-ref]
  (symbol (.getSchemaName table-ref) (.getTableName table-ref)))
