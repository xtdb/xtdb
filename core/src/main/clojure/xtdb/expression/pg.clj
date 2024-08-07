(ns xtdb.expression.pg
  (:require [xtdb.expression :as expr]
            [clojure.string :as str]
            [xtdb.error :as err]))

(set! *unchecked-math* :warn-on-boxed)

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
  (some #(when (contains? (:tables schema) %) %) (symbol-table-names tn)))

(defmethod expr/codegen-cast [:utf8 :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   #(do
      `(let [tn# (expr/buf->str ~@%)]
         (if-let [matching-tn# (find-matching-table-name ~expr/schema-sym tn#)]
           (hash matching-tn#)
           (throw (err/runtime-err ::unknown-relation
                                   {::err/message (format "Relation %s does not exist" tn#)})))))})

(defmethod expr/codegen-cast [:regclass :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   #(do
      `(let [oid# ~@%]
        (if-let [tn# (get-in ~expr/schema-sym [:oid->table oid#])]
          (expr/resolve-utf8-buf (string-table-name tn#))
          (expr/resolve-utf8-buf (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(do ~@%))})

(defmethod expr/codegen-cast [:int :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(do ~@%))})
