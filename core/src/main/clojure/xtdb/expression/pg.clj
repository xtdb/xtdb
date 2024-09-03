(ns xtdb.expression.pg
  (:require [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.information-schema :as info-schema])
  (:import [clojure.lang MapEntry]))

(defn symbol-table-names
  "returns possible symbol table names based on search path for unqualified names"
  [table]
  (let [parts (str/split table #"\.")]
    (if (= 1 (count parts))
      (mapv #(symbol % (first parts)) expr/search-path)
      [(symbol (str/join "." (butlast parts)) (last parts))])))

(defn string-table-name [table]
  (if (contains? (set expr/search-path) (namespace table))
    (name table)
    (str (namespace table) "." (name table))))

(defn find-matching-table-name [schema tn]
  (let [namespaced-schema (merge info-schema/table-info
                                 {'xt/txs #{"_id" "committed" "error" "system_time"}}
                                 (info-schema/namespace-public-tables schema))]
    (some #(when (contains? namespaced-schema %) %) (symbol-table-names tn))))

(defn table->oid [tn]
  (Math/abs ^Integer (hash tn)))

(defmethod expr/codegen-cast [:utf8 :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   (fn [[utf8-code]]
     `(let [tn# (expr/buf->str ~utf8-code)]
        (if-let [matching-tn# (find-matching-table-name ~expr/schema-sym tn#)]
          (table->oid matching-tn#)
          (throw (err/runtime-err ::unknown-relation
                                  {::err/message (format "Relation %s does not exist" tn#)})))))})

(defn ->oid->table [schema]
  (->> (merge info-schema/table-info
              {'xt/txs #{"_id" "committed" "error" "system_time"}}
              (info-schema/namespace-public-tables schema))
       (keys)
       (map #(MapEntry. (table->oid %) %))
       (into {})))

(defmethod expr/codegen-cast [:regclass :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   (fn [[regclass-code]]
     `(let [oid# ~regclass-code]
        (if-let [tn# (get (->oid->table ~expr/schema-sym) oid#)]
          (expr/resolve-utf8-buf (string-table-name tn#))
          (expr/resolve-utf8-buf (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defmethod expr/codegen-cast [:int :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})
