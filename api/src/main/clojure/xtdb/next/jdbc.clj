(ns xtdb.next.jdbc
  "This namespace contains several helper functions for working with XTDB and next.jdbc. "
  (:require [clojure.walk :as w]
            [next.jdbc.result-set :as nj-rs]
            [xtdb.serde :as serde])
  (:import clojure.lang.Keyword
           [java.sql ResultSet]
           [java.util List Map Set]
           xtdb.api.query.IKeyFn
           xtdb.util.NormalForm))

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

