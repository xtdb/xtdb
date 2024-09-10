(ns xtdb.expression.pg
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.information-schema :as info]))

(defn symbol-names
  "returns possible symbol names based on search path for unqualified names"
  [needle]
  (let [parts (str/split needle #"\.")]
    (if (= 1 (count parts))
      (mapv #(symbol % (first parts)) expr/search-path)
      [(symbol (str/join "." (butlast parts)) (last parts))])))

(defn string-name [fq-name]
  (when fq-name
    (if (contains? (set expr/search-path) (namespace fq-name))
      (name fq-name)
      (str (namespace fq-name) "." (name fq-name)))))

(defn table-names [schema]
  (set/union (set (keys info/table-info))
             (into #{'xt/txs} (map symbol) (keys schema))))

(defn find-matching-table-name [schema tn]
  (some (table-names schema)
        (symbol-names tn)))

(defn oid->table [schema oid]
  (->> (table-names schema)
       (filter #(= oid (info/name->oid %)))
       (first)))

(defmethod expr/codegen-cast [:utf8 :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[utf8-code]]
                  `(let [tn# (expr/buf->str ~utf8-code)]
                     (if-let [matching-tn# (find-matching-table-name ~expr/schema-sym tn#)]
                       (info/name->oid matching-tn#)
                       (throw (err/runtime-err ::unknown-relation
                                               {::err/message (format "Relation %s does not exist" tn#)})))))})

(defmethod expr/codegen-cast [:regclass :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[regclass-code]]
                  `(let [oid# ~regclass-code]
                     (expr/resolve-utf8-buf (or (string-name (oid->table ~expr/schema-sym oid#))
                                                (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defmethod expr/codegen-cast [:int :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code first})

(defmethod expr/codegen-cast [:utf8 :regproc] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[utf8-code]]
                  `(let [pn# (expr/buf->str ~utf8-code)]
                     (or (some (comp :oid info/procs) (symbol-names pn#))
                         (throw (err/runtime-err ::unknown-proc
                                                 {::err/message (format "Procedure %s does not exist" pn#)})))))})

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
