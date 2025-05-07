(ns xtdb.next.jdbc
  "This namespace contains several helper functions for working with XTDB and next.jdbc.

  Side effects - `require`-ing this namespace:
  - extends the next.jdbc protocols for XTDB nodes
  - implements `next.jdbc.result-set/ReadableColumn` for `PGobject`, using XTDB's extensions for Transit and JSON
  "
  (:require [clojure.walk :as w]
            [next.jdbc.result-set :as nj-rs]
            [xtdb.serde :as serde])
  (:import clojure.lang.Keyword
           java.nio.charset.StandardCharsets
           [java.sql ResultSet]
           [java.util List Map Set]
           org.postgresql.util.PGobject
           xtdb.api.query.IKeyFn
           xtdb.JsonSerde
           xtdb.util.NormalForm))

(defn ->pg-obj
  "This function serialises a Clojure/Java (potentially nested) data structure into a PGobject,
   in order to preserve the data structure's type information when stored in XTDB."
  [v]
  (doto (PGobject.)
    (.setType "transit")
    (.setValue (-> v
                   (->> (w/postwalk (fn [v]
                                      (cond-> v
                                        (map? v) (update-keys (fn [k]
                                                                (if (keyword? k)
                                                                  (NormalForm/normalForm ^Keyword k)
                                                                  k)))))))
                   (serde/write-transit :json)
                   (String. StandardCharsets/UTF_8)))))

(defn- denormalize-keys [v, ^IKeyFn key-fn]
  (w/prewalk (fn [v]
               (cond-> v
                 (instance? Map v) (update-keys #(.denormalize key-fn %))
                 (instance? List v) vec
                 (instance? Set v) set))
             v))

(defn ->sql-col
  "This function converts a keyword to an XT-normalised SQL column name."
  [^Keyword k]
  (NormalForm/normalForm k))

(defn ->label-fn [key-fn]
  (let [key-fn (serde/read-key-fn key-fn)]
    (fn [k]
      (.denormalize key-fn k))))

(def
  ^{:doc
    "This function converts an XT SQL column name to a kebab-cased keyword.

   * `_`-prefixed becomes `xt` namespaced: `_id` -> `:xt/id`, `_valid_from` -> `:xt/valid-from` etc
   * then, `_` -> `-`"}
  label-fn
  (->label-fn :kebab-case-keyword))

(defn ->col-reader
  "This col-reader recursively walks the values, denormalizing keys with the provided key-fn."
  [key-fn]

  (let [key-fn (serde/read-key-fn key-fn)]
    (fn col-reader [^ResultSet rs, rsmeta, ^long idx]
      (-> (nj-rs/read-column-by-index (.getObject rs idx) rsmeta idx)
          (denormalize-keys key-fn)))))

(def
  ^{:doc "This col-reader recursively converts result-set rows' map keys to kebab-cased keywords - see `label-fn` for more details."}
  col-reader (->col-reader :kebab-case-keyword))

(def
  ^{:doc "Builder function which recursively converts result-set rows' map keys to kebab-cased keywords - see `label-fn` for more details."}
  builder-fn
  (nj-rs/as-maps-adapter
   (fn [rs opts]
     (nj-rs/as-unqualified-modified-maps
      rs
      (assoc opts :label-fn label-fn)))
   col-reader))

(defmulti <-pg-obj
  (fn [^PGobject obj]
    (.getType obj)))

(defmethod <-pg-obj "transit" [^PGobject obj]
  (-> (.getValue obj)
      (.getBytes StandardCharsets/UTF_8)
      (serde/read-transit :json)))

(defmethod <-pg-obj "json" [^PGobject obj]
  (JsonSerde/decode (.getValue obj)))

(defmethod <-pg-obj "jsonb" [^PGobject obj]
  (JsonSerde/decode (.getValue obj)))

(defmethod <-pg-obj "keyword" [^PGobject obj]
  (keyword (.getValue obj)))

(defmethod <-pg-obj :default [^PGobject obj]
  obj)

(extend-protocol nj-rs/ReadableColumn
  PGobject
  (read-column-by-index [^PGobject obj _rs-meta _idx]
    (<-pg-obj obj)))

(try
  (Class/forName "xtdb.api.Xtdb")
  (require 'xtdb.next.jdbc.impls)
  (catch ClassNotFoundException _))
