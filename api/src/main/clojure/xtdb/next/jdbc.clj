(ns xtdb.next.jdbc
  "This namespace contains several helper functions for working with XTDB and next.jdbc. "
  (:require [clojure.walk :as w]
            [next.jdbc.result-set :as njrs]
            [xtdb.serde :as serde])
  (:import [java.sql ResultSet ResultSetMetaData]
           [java.util List Map Set]
           org.postgresql.PGConnection
           org.postgresql.jdbc.PgArray
           xtdb.api.query.IKeyFn))

(defn- read-col [{:keys [^IKeyFn key-fn]}, ^ResultSet rs, ^ResultSetMetaData rsmeta, ^long idx]
  (-> (njrs/read-column-by-index (.getObject rs idx) rsmeta idx)
      (as-> v (if (instance? PgArray v)
                (vec (.getArray ^PgArray v))
                v))
      (->> (w/prewalk (fn [v]
                        (cond-> v
                          (instance? Map v) (->> (into {} (keep (fn [[k v]]
                                                                  (when (some? v)
                                                                    [(.denormalize key-fn k) v])))))
                          (instance? List v) vec
                          (instance? Set v) set))))))

(defn builder-fn
  "Builder function which recursively converts result-set rows' map keys using the provided key-fn."
  [^ResultSet rs, opts]

  (let [rsmeta (.getMetaData rs)
        key-fn (serde/read-key-fn (::key-fn opts :kebab-case-keyword))
        read-opts {:key-fn key-fn}
        cols (mapv (fn [^Integer i]
                     (.denormalize key-fn (.getColumnLabel rsmeta i)))
                   (range 1 (inc (if rsmeta (.getColumnCount rsmeta) 0))))]

    (reify
      njrs/RowBuilder
      (->row [_this] (transient {}))
      (column-count [_this] (count cols))

      (with-column [this row i]
        (njrs/with-column-value this row (nth cols (dec i))
          (read-col read-opts rs rsmeta i)))

      (with-column-value [_this row col v]
        (cond-> row
          (some? v) (assoc! col v)))

      (row! [_this row] (persistent! row))

      njrs/ResultSetBuilder
      (->rs [_this] (transient []))

      (with-row [_this mrs row] (conj! mrs row))

      (rs! [_this mrs] (persistent! mrs)))))

(defn copy-in ^org.postgresql.copy.CopyIn [^PGConnection conn, sql]
  (-> (.getCopyAPI conn)
      (.copyIn sql)))
