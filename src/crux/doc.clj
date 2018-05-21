(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.db]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const sha1-size 20)

(def ^:const content-hash->doc-index-id 0)
(def ^:const attribute+value+content-hash-index-id 1)

(def empty-byte-array (byte-array 0))

(defn encode-doc-key ^bytes [^bytes content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES sha1-size))
      (.putShort content-hash->doc-index-id)
      (.put content-hash)
      (.array)))

(defn decode-doc-key ^bytes [^bytes doc-key]
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.getShort buffer)))
    (doto (byte-array sha1-size)
      (->> (.get buffer)))))

(defn encode-attribute+value+content-hash-key ^bytes [k v ^bytes content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES sha1-size sha1-size sha1-size))
      (.putShort attribute+value+content-hash-index-id)
      (.put (bu/sha1 (nippy/freeze k)))
      (.put (bu/sha1 (nippy/freeze v)))
      (.put content-hash)
      (.array)))

(defn encode-attribute+value-prefix-key ^bytes [k v]
  (-> (ByteBuffer/allocate (+ Short/BYTES sha1-size sha1-size))
      (.putShort attribute+value+content-hash-index-id)
      (.put (bu/sha1 (nippy/freeze k)))
      (.put (bu/sha1 (nippy/freeze v)))
      (.array)))

(defn decode-attribute+value-content-hash-key->content-hash ^bytes [^bytes attribute+value+content-hash-key]
  (let [buffer (ByteBuffer/wrap attribute+value+content-hash-key)]
    (assert (= attribute+value+content-hash-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES sha1-size sha1-size))
    (doto (byte-array sha1-size)
      (->> (.get buffer)))))

(defn key->bytes [k]
  (cond-> k
    (string? k) bu/hex->bytes))

(defn all-doc-keys [kv]
  (let [seek-k (.array (.putShort (ByteBuffer/allocate Short/BYTES) content-hash->doc-index-id))]
    (ks/iterate-with
     kv
     (fn [i]
       (loop [[k v :as kv] (ks/-seek i seek-k)
              acc #{}]
         (if (and kv (bu/bytes=? seek-k k))
           (let [content-hash (decode-doc-key k)]
             (recur (ks/-next i) (conj acc (bu/bytes->hex content-hash))))
           acc))))))

(defn entries [kv ks]
  (ks/iterate-with
   kv
   (fn [i]
     (set (for [seek-k (->> (map (comp encode-doc-key key->bytes) ks)
                            (into (sorted-set-by bu/bytes-comparator)))
                :let [[k v :as kv] (ks/-seek i seek-k)]
                :when (and k (bu/bytes=? seek-k k))]
            kv)))))

(defn docs [kv ks]
  (->> (for [[k v] (entries kv ks)]
         [(bu/bytes->hex (decode-doc-key k))
          (nippy/thaw v)])
       (into {})))

(defn existing-doc-keys [kv ks]
  (->> (for [[k v] (entries kv ks)]
         (bu/bytes->hex (decode-doc-key k)))
       (into #{})))

(defn store [kv docs]
  (let [content-hash->doc+bytes (->> (for [doc docs
                                           :let [bs (nippy/freeze doc)
                                                 k (bu/sha1 bs)]]
                                       [k [doc bs]])
                                     (into (sorted-map-by bu/bytes-comparator)))
        existing-keys (existing-doc-keys kv (keys content-hash->doc+bytes))
        content-hash->new-docs+bytes (apply dissoc content-hash->doc+bytes existing-keys)]
    (ks/store kv (concat
                  (for [[content-hash [doc bs]] content-hash->new-docs+bytes]
                    [(encode-doc-key content-hash)
                     bs])
                  (for [[content-hash [doc]] content-hash->new-docs+bytes
                        [k v] doc
                        v (if (or (vector? v)
                                  (set? v))
                            v
                            [v])]
                    [(encode-attribute+value+content-hash-key k v content-hash)
                     empty-byte-array])))
    (mapv bu/bytes->hex (keys content-hash->new-docs+bytes))))

(defn find-keys-by-attribute-values [kv k vs]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [seek-k (->> (for [v vs]
                              (encode-attribute+value-prefix-key k v))
                            (into (sorted-set-by bu/bytes-comparator)))]
            (loop [[k v :as kv] (ks/-seek i seek-k)
                   acc []]
              (if (and kv (bu/bytes=? seek-k k))
                (let [content-hash (decode-attribute+value-content-hash-key->content-hash k)]
                  (recur (ks/-next i) (conj acc (bu/bytes->hex content-hash))))
                acc)))
          (reduce into #{})))))

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

;; NOTE: this is a simple, non-temporal store using content hashes as ids.
(defrecord DocDatasource [kv]
  crux.db/Datasource
  (entities [this]
    (all-doc-keys kv))

  (entities-for-attribute-value [this ident v]
    (find-keys-by-attribute-values kv ident [v]))

  (attr-val [this eid ident]
    (get-in (docs kv [eid]) [eid ident])))
