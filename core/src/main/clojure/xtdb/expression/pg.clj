(ns xtdb.expression.pg
  (:require [clojure.string :as str]
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

(defn- schema-key->table [k]
  ;; table-info / schema keys are `[db-name table-ref]` qualified pairs; tolerate a bare ref/symbol too
  (table/ref->schema+table (if (vector? k) (second k) k)))

(defn table-oids
  "Every relation the query can see, as `{schema-qualified-symbol oid}`.

   The oid is the one the table's block records, resolved through `info/table-oid` so that a
   `::regclass` and the `pg_class` row for the same table come out equal — pgjdbc and Metabase resolve
   a name through the cast and then join on `pg_class.oid`."
  ;; TODO multi-db - a name present in two databases resolves to whichever the map happened to hold
  [schema]
  (-> (into {} (map (fn [k] (let [table (schema-key->table k)]
                              [table (info/table-oid nil table)])))
            (keys (info/table-info "xtdb")))
      (into (map (fn [[k {:keys [oid]}]] (let [table (schema-key->table k)]
                                           [table (info/table-oid oid table)])))
            schema)))

(defn find-table [schema table-name]
  (or (some (table-oids schema) (symbol-names table-name))
      (throw (err/incorrect ::unknown-relation (format "Relation %s does not exist" table-name)))))

(defmethod expr/codegen-cast [:utf8 :regclass] [_]
  {:return-type #xt/type :regclass
   :->call-code (fn [[utf8-code]]
                  `(find-table ~expr/schema-sym (expr/buf->str ~utf8-code)))})

(defn oid->table [schema oid]
  (->> (table-oids schema)
       (some (fn [[table oid']] (when (= oid oid') table)))))

(defn string-name [fq-name]
  (when fq-name
    (if (contains? (set expr/search-path) (namespace fq-name))
      (name fq-name)
      (str (namespace fq-name) "." (name fq-name)))))

(defmethod expr/codegen-cast [:regclass :utf8] [_]
  {:return-type #xt/type :utf8
   :->call-code (fn [[regclass-code]]
                  `(let [oid# ~regclass-code]
                     (expr/resolve-utf8-buf (or (some-> (oid->table ~expr/schema-sym oid#) table/ref->schema+table string-name)
                                                (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [_]
  {:return-type #xt/type :i32
   :->call-code first})

(defmethod expr/codegen-cast [:int :regclass] [_]
  {:return-type #xt/type :regclass
   :->call-code first})

(defn find-proc [proc-name]
  (or (some (comp :oid info/procs) (symbol-names proc-name))
      (throw (err/incorrect ::unknown-proc (format "Procedure %s does not exist" proc-name)))))

(defmethod expr/codegen-cast [:utf8 :regproc] [_]
  {:return-type #xt/type :regproc
   :->call-code (fn [[utf8-code]]
                  `(find-proc (expr/buf->str ~utf8-code)))})

(defmethod expr/codegen-cast [:regproc :utf8] [_]
  {:return-type #xt/type :utf8
   :->call-code (fn [[regproc-code]]
                  `(let [oid# ~regproc-code]
                     (expr/resolve-utf8-buf (or (string-name (info/oid->proc oid#))
                                                (str oid#)))))})

(defmethod expr/codegen-cast [:regproc :int] [_]
  {:return-type #xt/type :i32
   :->call-code first})

(defmethod expr/codegen-cast [:int :regproc] [_]
  {:return-type #xt/type :regproc
   :->call-code first})

;;; oid casts

(defmethod expr/codegen-cast [:int :oid] [_]
  {:return-type #xt/type :oid
   :->call-code first})

(defmethod expr/codegen-cast [:oid :int] [_]
  {:return-type #xt/type :i32
   :->call-code first})

(defmethod expr/codegen-cast [:regclass :oid] [_]
  {:return-type #xt/type :oid
   :->call-code first})

(defmethod expr/codegen-cast [:regproc :oid] [_]
  {:return-type #xt/type :oid
   :->call-code first})

;;; col_description

(defmethod expr/codegen-call [:col_description :int :int] [expr]
  (expr/codegen-call (-> expr
                         (assoc-in [:arg-types 0] #xt/type :oid))))

(defmethod expr/codegen-call [:col_description :oid :int] [_]
  {:return-type #xt/type :null
   :->call-code (fn [[_regoid-code _int-code]]
                  nil)})
