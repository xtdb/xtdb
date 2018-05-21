(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const sha1-size 20)

(def ^:const doc-index-id 0)

(defn encode-doc-key ^bytes [^bytes content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES sha1-size))
      (.putShort doc-index-id)
      (.put content-hash)
      (.array)))

(defn decode-doc-key ^bytes [^bytes doc-key]
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= doc-index-id (.getShort buffer)))
    (doto (byte-array sha1-size)
      (->> (.get buffer)))))

(defn store [kv docs]
  (let [kvs (->> (for [doc docs
                       :let [v (nippy/freeze doc)
                             k (encode-doc-key (bu/sha1 v))]]
                   [k v])
                 (into {}))]
    (ks/store kv kvs)
    (mapv (comp bu/bytes->hex decode-doc-key) (keys kvs))))

(defn key->bytes [k]
  (cond-> k
    (string? k) bu/hex->bytes))

(defn docs [kv ks]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [seek-k (->> (map (comp encode-doc-key key->bytes) ks)
                            (into (sorted-set-by bu/bytes-comparator)))
                :let [[k v] (ks/-seek i seek-k)]
                :when (and k (bu/bytes=? seek-k k))]
            [(bu/bytes->hex (decode-doc-key k))
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
