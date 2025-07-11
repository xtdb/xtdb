(ns xtdb.expression.pg
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.information-schema :as info]
            [xtdb.table :as table]))

(defn symbol-names
  "returns possible symbol names based on search path for unqualified names"
  [needle]
  (let [needle-sym (symbol (str/replace needle #"\." "/"))]
    (if (qualified-symbol? needle-sym)
      [needle-sym]
      (for [schema expr/search-path]
        (symbol schema needle)))))

(defn tables [schema]
  (set/union (set (keys info/table-info))
             #{#xt/table xt/txs}
             (set (keys schema))))

(defn find-table [schema table-name]
  (or (some-> (some (tables schema) (->> (symbol-names table-name)
                                         (map table/->ref)))
              table/ref->sym
              (info/name->oid))
      (throw (err/incorrect ::unknown-relation (format "Relation %s does not exist" table-name)))))

(defmethod expr/codegen-cast [:utf8 :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[utf8-code]]
                  `(find-table ~expr/schema-sym (expr/buf->str ~utf8-code)))})

(defn oid->table [schema oid]
  (->> (tables schema)
       (filter #(= oid (info/name->oid (table/ref->sym %))))
       (first)))

(defn string-name [fq-name]
  (when fq-name
    (if (contains? (set expr/search-path) (namespace fq-name))
      (name fq-name)
      (str (namespace fq-name) "." (name fq-name)))))

(defmethod expr/codegen-cast [:regclass :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[regclass-code]]
                  `(let [oid# ~regclass-code]
                     (expr/resolve-utf8-buf (or (some-> (oid->table ~expr/schema-sym oid#) table/ref->sym string-name)
                                                (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defmethod expr/codegen-cast [:int :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defn find-proc [proc-name]
  (or (some (comp :oid info/procs) (symbol-names proc-name))
      (throw (err/incorrect ::unknown-proc (format "Procedure %s does not exist" proc-name)))))

(defmethod expr/codegen-cast [:utf8 :regproc] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[utf8-code]]
                  `(find-proc (expr/buf->str ~utf8-code)))})

(defmethod expr/codegen-cast [:regproc :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[regproc-code]]
                  `(let [oid# ~regproc-code]
                     (expr/resolve-utf8-buf (or (string-name (info/oid->proc oid#))
                                                (str oid#)))))})

(defmethod expr/codegen-cast [:regproc :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defmethod expr/codegen-cast [:int :regproc] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})
