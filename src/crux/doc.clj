(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.db]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Date LinkedHashMap]
           [java.util.function Function]))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^:const ^:private content-hash->doc-index-id 0)
(def ^:const ^:private attribute+value+content-hash-index-id 1)

(def ^:const ^:private content-hash+entity-index-id 2)
(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def ^:private empty-byte-array (byte-array 0))
(def ^:const ^:private sha1-size (.getDigestLength (MessageDigest/getInstance "SHA-1")))

(defn encode-keyword ^bytes [kw]
  (bu/sha1 (.getBytes (str kw))))

(defn- encode-doc-key ^bytes [^bytes content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES (alength content-hash)))
      (.putShort content-hash->doc-index-id)
      (.put content-hash)
      (.array)))

(defn- decode-doc-key ^bytes [^bytes doc-key]
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.getShort buffer)))
    (doto (byte-array sha1-size)
      (->> (.get buffer)))))

(defn- encode-attribute+value+content-hash-key ^bytes [k v ^bytes content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES sha1-size sha1-size (alength content-hash)))
      (.putShort attribute+value+content-hash-index-id)
      (.put (encode-keyword k))
      (.put (if (keyword? v)
              (encode-keyword v)
              (bu/sha1 (nippy/fast-freeze v))))
      (.put content-hash)
      (.array)))

(defn- encode-attribute+value-prefix-key ^bytes [k v]
  (encode-attribute+value+content-hash-key k v empty-byte-array))

(defn- decode-attribute+value+content-hash-key->content-hash ^bytes [^bytes key]
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= attribute+value+content-hash-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES sha1-size sha1-size))
    (doto (byte-array sha1-size)
      (->> (.get buffer)))))

(defn- encode-content-hash+entity-key ^bytes [^bytes content-hash ^bytes eid]
  (-> (ByteBuffer/allocate (+ Short/BYTES (alength content-hash) (alength eid)))
      (.putShort content-hash+entity-index-id)
      (.put content-hash)
      (.put eid)
      (.array)))

(defn- encode-content-hash-prefix-key ^bytes [^bytes content-hash]
  (encode-content-hash+entity-key content-hash empty-byte-array))

(defn- decode-content-hash+entity-key->entity ^bytes [^bytes key]
    (let [buffer (ByteBuffer/wrap key)]
      (assert (= content-hash+entity-index-id (.getShort buffer)))
      (.position buffer (+ Short/BYTES sha1-size))
      (doto (byte-array sha1-size)
        (->> (.get buffer)))))

(defn- encode-meta-key ^bytes [k]
  (let [k (encode-keyword k)]
    (-> (ByteBuffer/allocate (+ Short/BYTES (alength k)))
        (.putShort meta-key->value-index-id)
        (.put k)
        (.array))))

(def ^:const max-timestamp ^:private (.getTime #inst "9999-12-30"))

(defn- date->reverse-time-ms ^long [^Date date]
  (- max-timestamp (.getTime date)))

(defn- ^Date reverse-time-ms->date [^long reverse-time-ms]
  (Date. (- max-timestamp reverse-time-ms)))

(defn- encode-entity+bt+tt+tx-id-key ^bytes [^bytes eid ^Date business-time ^Date transact-time ^long tx-id]
  (let [tx-id-size (if (pos? tx-id)
                     (long Long/BYTES)
                     0)]
    (cond-> (ByteBuffer/allocate (+ Short/BYTES (alength eid) Long/BYTES Long/BYTES tx-id-size))
      true (-> (.putShort entity+bt+tt+tx-id->content-hash-index-id)
               (.put eid)
               (.putLong (date->reverse-time-ms business-time))
               (.putLong (date->reverse-time-ms transact-time)))
      (= Long/BYTES tx-id-size) (.putLong tx-id)
      true (.array))))

(defn- encode-entity+bt+tt-prefix-key
  (^bytes []
   (-> (ByteBuffer/allocate Short/BYTES)
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.array)))
  (^bytes [^bytes eid ^Date business-time ^Date transact-time]
   (encode-entity+bt+tt+tx-id-key eid business-time transact-time -1)))

(defn- decode-entity+bt+tt+tx-id-key ^bytes [^bytes key]
    (let [buffer (ByteBuffer/wrap key)]
      (assert (= entity+bt+tt+tx-id->content-hash-index-id (.getShort buffer)))
      {:eid (doto (byte-array sha1-size)
              (->> (.get buffer)))
       :bt (reverse-time-ms->date (.getLong buffer))
       :tt (reverse-time-ms->date (.getLong buffer))
       :tx-id (.getLong buffer)}))

;; Caching

(defn lru-cache [^long size]
  (proxy [LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [_]
      (> (count this) size))))

(defn lru-cache-compute-if-absent [^LinkedHashMap cache k f]
  (.computeIfAbsent cache k (reify Function
                              (apply [_ k]
                                (f k)))))

(defn lru-named-cache [state cache-name cache-size]
  (or (get @state cache-name)
      (get (swap! state assoc cache-name (lru-cache cache-size)) cache-name)))

;; Docs

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

(defn- doc-key->bytes [k]
  (cond-> k
    (string? k) (bu/hex->bytes)))

(defn doc-entries
  ([kv ks]
   (ks/iterate-with
    kv
    (fn [i]
      (doc-entries i kv ks))))
  ([i kv ks]
   (set (for [seek-k (->> (map (comp encode-doc-key doc-key->bytes) ks)
                          (sort bu/bytes-comparator))
              :let [[k v :as kv] (ks/-seek i seek-k)]
              :when (and kv (bu/bytes=? seek-k k))]
          kv))))

(defn docs
  ([kv ks]
   (ks/iterate-with
    kv
    (fn [i]
      (docs i kv ks))))
  ([i kv ks]
   (->> (for [[k v] (doc-entries i kv ks)]
          [(bu/bytes->hex (decode-doc-key k))
           (nippy/fast-thaw v)])
        (into {}))))

(defn existing-doc-keys
  ([kv ks]
   (ks/iterate-with
    kv
    (fn [i]
      (existing-doc-keys i kv ks))))
  ([i kv ks]
   (->> (for [[k v] (doc-entries i kv ks)]
          (bu/bytes->hex (decode-doc-key k)))
        (into #{}))))

(defn doc-keys-by-attribute-values
  ([kv k vs]
   (ks/iterate-with
    kv
    (fn [i]
      (doc-keys-by-attribute-values i kv k vs))))
  ([i kv k vs]
   (->> (for [v vs]
          (encode-attribute+value-prefix-key k v))
        (sort bu/bytes-comparator)
        (reduce
         (fn [acc seek-k]
           (loop [[k v :as kv] (ks/-seek i seek-k)
                  acc acc]
             (if (and kv (bu/bytes=? seek-k k))
               (recur (ks/-next i)
                      (->> (decode-attribute+value+content-hash-key->content-hash k)
                           (bu/bytes->hex)
                           (conj acc)))
               acc)))
         #{}))))

(defn doc->content-hash [doc]
  (bu/sha1 (nippy/fast-freeze doc)))

(defn store-docs [kv docs]
  (let [content-hash->doc+bytes (->> (for [doc docs
                                           :let [bs (nippy/fast-freeze doc)
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
                        v (cond-> v
                            (not (or (vector? v)
                                     (set? v))) (vector))]
                    [(encode-attribute+value+content-hash-key k v content-hash)
                     empty-byte-array])))
    (mapv bu/bytes->hex (keys content-hash->new-docs+bytes))))

;; Txs Read

(defn- entity->eid-bytes ^bytes [k]
  (cond
    (bytes? k)
    k

    (and (string? k)
         (even? (count k))
         (re-find #"\p{XDigit}+" k))
    (bu/hex->bytes k)

    (keyword? k)
    (encode-keyword k)

    :else
    (bu/sha1 (nippy/fast-freeze k))))

(defn eids-by-content-hashes
  ([kv content-hashes]
   (ks/iterate-with
    kv
    (fn [i]
      (eids-by-content-hashes i kv content-hashes))))
  ([i kv content-hashes]
   (->> (for [content-hash content-hashes]
          [(encode-content-hash-prefix-key (bu/hex->bytes content-hash))
           content-hash])
        (into (sorted-map-by bu/bytes-comparator))
        (reduce-kv
         (fn [acc seek-k content-hash]
           (loop [[k v :as kv] (ks/-seek i seek-k)
                  acc acc]
             (if (and kv (bu/bytes=? seek-k k))
               (recur (ks/-next i)
                      (update acc
                              content-hash
                              conj
                              (bu/bytes->hex (decode-content-hash+entity-key->entity k))))
               acc)))
         {}))))

(defn entities-at
  ([kv entities business-time transact-time]
   (ks/iterate-with
    kv
    (fn [i]
      (entities-at i kv entities business-time transact-time))))
  ([i kv entities business-time transact-time]
   (let [prefix-size (+ Short/BYTES sha1-size)]
     (->> (for [seek-k (->> (for [entity entities]
                              (encode-entity+bt+tt-prefix-key
                               (entity->eid-bytes entity)
                               business-time
                               transact-time))
                            (sort bu/bytes-comparator))
                :let [entity-map (loop [[k v :as kv] (ks/-seek i seek-k)]
                                   (when (and kv
                                              (bu/bytes=? seek-k prefix-size k)
                                              (pos? (alength ^bytes v)))
                                     (let [entity-map (-> (decode-entity+bt+tt+tx-id-key k)
                                                          (assoc :content-hash (bu/bytes->hex v))
                                                          (update :eid bu/bytes->hex))]
                                       (if (<= (compare (:tt entity-map) transact-time) 0)
                                         entity-map
                                         (recur (ks/-next i))))))]
                :when entity-map]
            [(:eid entity-map) entity-map])
          (into {})))))


(defn entities-by-attribute-values-at [kv k vs business-time transact-time]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [[content-hash eids] (->> (doc-keys-by-attribute-values i kv k vs)
                                         (eids-by-content-hashes i kv))
                [eid entity-map] (entities-at i kv eids business-time transact-time)
                :when (= content-hash (:content-hash entity-map))]
            [eid entity-map])
          (into {})))))

(defn all-entities [kv business-time transact-time]
  (let [seek-k (encode-entity+bt+tt-prefix-key)]
    (ks/iterate-with
     kv
     (fn [i]
       (loop [[k v :as kv] (ks/-seek i seek-k)
              acc #{}]
         (if (and kv (bu/bytes=? seek-k k))
           (let [{:keys [eid]} (decode-entity+bt+tt+tx-id-key k)]
             (recur (ks/-next i) (conj acc (bu/bytes->hex eid))))
           (entities-at i kv acc business-time transact-time)))))))

;; Tx Commands

(defmulti tx-command (fn [kv [op] transact-time tx-id] op))

(defmethod tx-command :crux.tx/put [kv [op k v business-time] transact-time tx-id]
  (let [eid (entity->eid-bytes k)
        content-hash (bu/hex->bytes v)
        business-time (or business-time transact-time)]
    [[(encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      content-hash]
     [(encode-content-hash+entity-key content-hash eid)
      empty-byte-array]]))

(defmethod tx-command :crux.tx/delete [kv [op k business-time] transact-time tx-id]
  (let [eid (entity->eid-bytes k)
        business-time (or business-time transact-time)]
    [[(encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      empty-byte-array]]))

(defmethod tx-command :crux.tx/cas [kv [op k old-v new-v business-time] transact-time tx-id]
  (let [eid (entity->eid-bytes k)
        old-content-hash (-> (entities-at kv [k] business-time transact-time)
                             (get k)
                             :content-hash)
        business-time (or business-time transact-time)]
    (when (= old-content-hash old-v)
      [[(encode-entity+bt+tt+tx-id-key
         eid
         business-time
         transact-time
         tx-id)
        new-v]
       [(encode-content-hash+entity-key new-v eid)
        empty-byte-array]])))

(defn store-txs [kv commands transact-time tx-id]
  (->> (for [command commands]
         (tx-command kv command transact-time tx-id))
       (reduce into {})
       (ks/store kv)))

;; Query

(def ^:const default-doc-cache-size 10240)

(defrecord DocDatasource [kv business-time transact-time]
  crux.db/Datasource
  (entities [this]
    (keys (all-entities kv business-time transact-time)))

  (entities-for-attribute-value [this ident v]
    (keys (entities-by-attribute-values-at kv ident [v] business-time transact-time)))

  (attr-val [this eid ident]
    (ks/iterate-with
     kv
     (fn [i]
       (let [content-hash (get-in (entities-at i kv [eid] business-time transact-time) [eid :content-hash])
             doc (-> (lru-named-cache (:state kv) :doc-cache default-doc-cache-size)
                     (lru-cache-compute-if-absent
                      content-hash
                      #(get (docs i kv [%]) %)))]
         (get-in doc [eid ident]))))))
