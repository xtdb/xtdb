(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.kv-store-utils :as kvu]
            [taoensso.nippy :as nippy]))

(defn store [kv docs]
  (let [kvs (->> (for [doc docs
                       :let [v (nippy/freeze doc)
                             k (bu/sha1 v)]]
                   [k v])
                 (into {}))]
    (ks/store kv kvs)
    (mapv bu/bytes->hex (keys kvs))))

(defn key->bytes [k]
  (cond-> k
    (string? k) bu/hex->bytes))

(defn values [kv ks]
  (ks/iterate-with
   kv
   (fn [i]
     (vec (for [seek-k (sort-by bu/bytes-comparator (map key->bytes ks))
                :let [[k v] (ks/-seek i seek-k)]
                :when (bu/bytes=? seek-k k)]
            (nippy/thaw v))))))

(defn tx-put
  ([k v]
   (tx-put k v nil))
  ([k v business-time]
   (cond-> [:crux.tx/put k v]
     business-time (conj business-time))))
