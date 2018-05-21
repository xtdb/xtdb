(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
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

(defn docs [kv ks]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [seek-k (into (sorted-set-by bu/bytes-comparator) (map key->bytes ks))
                :let [[k v] (ks/-seek i seek-k)]
                :when (bu/bytes=? seek-k k)]
            [(bu/bytes->hex k)
             (nippy/thaw v)])
          (into {})))))

(defn tx-put
  ([k v]
   (tx-put k v nil))
  ([k v business-time]
   (cond-> [:crux.tx/put k v]
     business-time (conj business-time))))

(defn tx-cas
  ([k v-old v-new]
   (tx-cas k v-old v-new nil))
  ([k v-old v-new business-time]
   (cond-> [:crux.tx/cas k v-old v-new]
     business-time (conj business-time))))

(defn tx-delete
  ([k]
   (tx-delete k nil))
  ([k business-time]
   (cond-> [:crux.tx/delete k]
     business-time (conj business-time))))
