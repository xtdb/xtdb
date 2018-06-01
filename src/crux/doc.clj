(ns crux.doc
  (:require [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.kv-store-utils :as kvu]
            [crux.db]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Arrays Date LinkedHashMap UUID]
           [java.util.function Function]
           [clojure.lang Keyword]))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^:const ^:private content-hash->doc-index-id 0)
(def ^:const ^:private attribute+value+content-hash-index-id 1)

(def ^:const ^:private content-hash+entity-index-id 2)
(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def ^:private empty-byte-array (byte-array 0))
(def ^:const ^:private id-hash-algorithm "SHA-1")
(def ^:const ^:private id-size (.getDigestLength (MessageDigest/getInstance id-hash-algorithm)))

(defprotocol ValueToBytes
  (value->bytes ^bytes [this]))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBytes
  (class (byte-array 0))
  (value->bytes [this]
    this)

  Long
  (value->bytes [this]
    (bu/long->bytes (bit-xor ^long this Long/MIN_VALUE)))

  Double
  (value->bytes [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (bu/long->bytes l)))

  Date
  (value->bytes [this]
    (value->bytes (.getTime this)))

  String
  (value->bytes [this]
    (let [empty-mark (byte 0)
          terminate-mark (byte 1)
          offset (byte 2)]
      (if (empty? this)
        (byte-array [empty-mark])
        (let [bs (.getBytes this "UTF-8")
              buffer (ByteBuffer/allocate (inc (alength bs)))]
          (doseq [^byte b bs]
            (.put buffer (byte (+ offset b))))
          (-> buffer
              (.put terminate-mark)
              (.array))))))

  Object
  (value->bytes [this]
    (bu/sha1 (nippy/fast-freeze this))))

(defprotocol IdToBytes
  (id->bytes ^bytes [this]))

(def ^:private hex-id-pattern
  (re-pattern (format "\\p{XDigit}{%d}" (* 2 id-size))))

(extend-protocol IdToBytes
  (class (byte-array 0))
  (id->bytes [this]
    this)

  ByteBuffer
  (id->bytes [this]
    (.array this))

  Keyword
  (id->bytes [this]
    (bu/sha1 (.getBytes (str this))))

  UUID
  (id->bytes [this]
    (bu/sha1 (.getBytes (str this))))

  String
  (id->bytes [this]
    (if (re-find hex-id-pattern this)
      (bu/hex->bytes this)
      (throw (IllegalArgumentException. (format "Not a %s hex string: %s" id-hash-algorithm this)))))

  Object
  (id->bytes [this]
    (throw (UnsupportedOperationException.))))

(deftype Id [^bytes bytes]
  IdToBytes
  (id->bytes [this]
    (id->bytes bytes))

  Object
  (toString [this]
    (bu/bytes->hex bytes))

  (equals [this that]
    (and (instance? Id that)
         (Arrays/equals bytes ^bytes (.bytes ^Id that))))

  (hashCode [this]
    (Arrays/hashCode bytes))

  Comparable
  (compareTo [this that]
    (bu/compare-bytes bytes (.bytes ^Id that))))

(defn- encode-doc-key ^bytes [^bytes content-hash]
  (assert (= id-size (alength content-hash)))
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort content-hash->doc-index-id)
      (.put content-hash)
      (.array)))

(defn- decode-doc-key ^bytes [^bytes doc-key]
  (assert (= (+ Short/BYTES id-size) (alength doc-key)))
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.getShort buffer)))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn- encode-attribute+value+content-hash-key ^bytes [k v ^bytes content-hash]
  (assert (or (= id-size (alength content-hash))
              (zero? (alength content-hash))))
  (let [v (value->bytes v)]
    (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength v) (alength content-hash)))
        (.putShort attribute+value+content-hash-index-id)
        (.put (id->bytes k))
        (.put v)
        (.put content-hash)
        (.array))))

(defn- encode-attribute+value-prefix-key ^bytes [k v]
  (encode-attribute+value+content-hash-key k v empty-byte-array))

(defn- decode-attribute+value+content-hash-key->content-hash ^bytes [^bytes key]
  (assert (<= (+ Short/BYTES id-size id-size) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= attribute+value+content-hash-index-id (.getShort buffer)))
    (.position buffer (- (alength key) id-size))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn- encode-content-hash+entity-key ^bytes [^bytes content-hash ^bytes eid]
  (assert (= id-size (alength content-hash)))
  (assert (or (= id-size (alength eid))
              (zero? (alength eid))))
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength eid)))
      (.putShort content-hash+entity-index-id)
      (.put content-hash)
      (.put eid)
      (.array)))

(defn- encode-content-hash-prefix-key ^bytes [^bytes content-hash]
  (encode-content-hash+entity-key content-hash empty-byte-array))

(defn- decode-content-hash+entity-key->entity ^bytes [^bytes key]
  (assert (= (+ Short/BYTES id-size id-size) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= content-hash+entity-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES id-size))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn- encode-meta-key ^bytes [k]
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort meta-key->value-index-id)
      (.put (id->bytes k))
      (.array)))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- ^Date reverse-time-ms->date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn- encode-entity+bt+tt+tx-id-key ^bytes [^bytes eid ^Date business-time ^Date transact-time ^long tx-id]
  (assert (= id-size (alength eid)))
  (let [tx-id-size (if (pos? tx-id)
                     (long Long/BYTES)
                     0)]
    (cond-> (ByteBuffer/allocate (+ Short/BYTES id-size Long/BYTES Long/BYTES tx-id-size))
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
  (assert (= (+ Short/BYTES id-size Long/BYTES Long/BYTES Long/BYTES)) (alength key))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id (.getShort buffer)))
    {:eid (doto (byte-array id-size)
            (->> (.get buffer)))
     :bt (reverse-time-ms->date (.getLong buffer))
     :tt (reverse-time-ms->date (.getLong buffer))
     :tx-id (.getLong buffer)}))

(defn- all-keys-in-index [kv index-id]
  (ks/iterate-with
   kv
   (fn [i]
     (let [seek-k (.array (.putShort (ByteBuffer/allocate Short/BYTES) index-id))]
       (loop [[k v] (ks/-seek i seek-k)
              acc #{}]
         (if (and k (bu/bytes=? seek-k k))
           (recur (ks/-next i) (conj acc k))
           acc))))))

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
  (get (swap! state
              update
              cache-name
              (fn [cache]
                (or cache (lru-cache cache-size))))
       cache-name))

;; Docs

(defn all-doc-keys [kv]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (all-keys-in-index kv content-hash->doc-index-id)
          (map (comp ->Id decode-doc-key))
          (set)))))

(defn docs [kv ks]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [seek-k (->> (map (comp encode-doc-key id->bytes) ks)
                            (sort bu/bytes-comparator))
                :let [[k v] (ks/-seek i seek-k)]
                :when (and k (bu/bytes=? seek-k k))]
            [(->Id (decode-doc-key k))
             (ByteBuffer/wrap v)])
          (into {})))))

(defn doc-keys-by-attribute-values [kv k vs]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [v vs]
            (if (vector? v)
              (let [[min-v max-v] v]
                (if max-v
                  (assert (not (neg? (compare max-v min-v))))
                  (assert min-v))
                [(encode-attribute+value-prefix-key k min-v)
                 (encode-attribute+value-prefix-key k (or max-v empty-byte-array))])
              (let [seek-k (encode-attribute+value-prefix-key k v)]
                [seek-k seek-k])))
          (sort-by first bu/bytes-comparator)
          (reduce
           (fn [acc [min-seek-k ^bytes max-seek-k]]
             (loop [[k v] (ks/-seek i min-seek-k)
                    acc acc]
               (if (and k (not (neg? (bu/compare-bytes max-seek-k k (alength max-seek-k)))))
                 (recur (ks/-next i)
                        (->> (decode-attribute+value+content-hash-key->content-hash k)
                             (->Id)
                             (conj acc)))
                 acc)))
           #{})))))

(defn ^Id doc->content-hash [doc]
  (->Id (bu/sha1 (nippy/fast-freeze doc))))

(defn existing-doc-keys [kv ks]
  (->> (docs kv ks)
       (keys)
       (into #{})))

(defn store-docs [kv docs]
  (let [content-hash+doc+bytes (for [doc docs
                                     :let [doc-bytes (nippy/fast-freeze doc)
                                           k (bu/sha1 doc-bytes)]]
                                 [k [doc doc-bytes]])
        new-keys (map first content-hash+doc+bytes)
        existing-keys (existing-doc-keys kv new-keys)
        content-hash->new-docs+bytes (apply dissoc (into {} content-hash+doc+bytes) existing-keys)]
    (ks/store kv (concat
                  (for [[content-hash [doc doc-bytes]] content-hash->new-docs+bytes]
                    [(encode-doc-key content-hash)
                     doc-bytes])
                  (for [[content-hash [doc]] content-hash->new-docs+bytes
                        [k v] doc
                        v (cond-> v
                            (not (or (vector? v)
                                     (set? v))) (vector))]
                    [(encode-attribute+value+content-hash-key k v content-hash)
                     empty-byte-array])))
    (map (comp ->Id first) content-hash+doc+bytes)))

;; Txs Read

(defn entities-at [kv entities business-time transact-time]
  (ks/iterate-with
   kv
   (fn [i]
     (let [prefix-size (+ Short/BYTES id-size)]
       (->> (for [seek-k (->> (for [entity entities]
                                (encode-entity+bt+tt-prefix-key
                                 (id->bytes entity)
                                 business-time
                                 transact-time))
                              (sort bu/bytes-comparator))
                  :let [entity-map (loop [[k v] (ks/-seek i seek-k)]
                                     (when (and k
                                                (bu/bytes=? seek-k prefix-size k)
                                                (pos? (alength ^bytes v)))
                                       (let [entity-map (decode-entity+bt+tt+tx-id-key k)]
                                         (if (<= (compare (:tt entity-map) transact-time) 0)
                                           (-> entity-map
                                               (assoc :content-hash (->Id v))
                                               (update :eid ->Id))
                                           (recur (ks/-next i))))))]
                  :when entity-map]
              [(:eid entity-map) entity-map])
            (into {}))))))


(defn eids-by-content-hashes [kv content-hashes]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [content-hash content-hashes
                :let [content-hash (id->bytes content-hash)]]
            [(encode-content-hash-prefix-key content-hash)
             (->Id content-hash)])
          (into (sorted-map-by bu/bytes-comparator))
          (reduce-kv
           (fn [acc seek-k content-hash]
             (loop [[k v] (ks/-seek i seek-k)
                    acc acc]
               (if (and k (bu/bytes=? seek-k k))
                 (recur (ks/-next i)
                        (update acc
                                content-hash
                                conj
                                (->Id (decode-content-hash+entity-key->entity k))))
                 acc)))
           {})))))

(defn entities-by-attribute-values-at [kv k vs business-time transact-time]
  (ks/iterate-with
   kv
   (fn [i]
     (->> (for [[content-hash eids] (->> (doc-keys-by-attribute-values kv k vs)
                                         (eids-by-content-hashes kv))
                [eid entity-map] (entities-at kv eids business-time transact-time)
                :when (= content-hash (:content-hash entity-map))]
            [eid entity-map])
          (into {})))))

(defn all-entities [kv business-time transact-time]
  (ks/iterate-with
   kv
   (fn [i]
     (let [eids (->> (all-keys-in-index kv entity+bt+tt+tx-id->content-hash-index-id)
                     (map (comp ->Id :eid decode-entity+bt+tt+tx-id-key)))]
       (entities-at kv eids business-time transact-time)))))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(encode-meta-key k)
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (some->> ^bytes (kvu/value kv (encode-meta-key k))
           nippy/fast-thaw))

;; Tx Commands

(s/def ::id (s/conformer (comp str ->Id id->bytes)))
(s/def ::doc (s/and (s/or :doc (s/and map? (s/conformer (comp str doc->content-hash)))
                          :content-hash ::id)
                    (s/conformer second)))

(s/def ::put-op (s/cat :op #{:crux.tx/put}
                       :id ::id
                       :doc ::doc
                       :business-time (s/? inst?)))

(s/def ::delete-op (s/cat :op #{:crux.tx/delete}
                          :id ::id
                          :business-time (s/? inst?)))

(s/def ::cas-op (s/cat :op #{:crux.tx/cas}
                       :id ::id
                       :old-doc ::doc
                       :new-doc ::doc
                       :business-time (s/? inst?)))

(s/def ::tx-op (s/and (s/or :put ::put-op
                            :delete ::delete-op
                            :cas ::cas-op)
                      (s/conformer (comp vec vals second))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind vector?))

(defmulti tx-command (fn [kv [op] transact-time tx-id] op))

(defmethod tx-command :crux.tx/put [kv [op k v business-time] transact-time tx-id]
  (let [eid (id->bytes k)
        content-hash (id->bytes v)
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
  (let [eid (id->bytes k)
        business-time (or business-time transact-time)]
    [[(encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      empty-byte-array]]))

(defmethod tx-command :crux.tx/cas [kv [op k old-v new-v business-time] transact-time tx-id]
  (let [eid (id->bytes k)
        old-content-hash (-> (entities-at kv [k] business-time transact-time)
                             (get k)
                             :content-hash)
        business-time (or business-time transact-time)
        old-v (id->bytes old-v)
        new-v (id->bytes new-v)]
    (when (bu/bytes=? old-content-hash old-v)
      [[(encode-entity+bt+tt+tx-id-key
         eid
         business-time
         transact-time
         tx-id)
        new-v]
       [(encode-content-hash+entity-key new-v eid)
        empty-byte-array]])))

(defn store-txs [kv tx-ops transact-time tx-id]
  (->> (for [tx-op tx-ops]
         (tx-command kv tx-op transact-time tx-id))
       (reduce into {})
       (ks/store kv)))

(defrecord DocIndexer [kv]
  crux.db/Indexer
  (index [_ tx-ops transact-time tx-id]
    (store-txs kv tx-ops transact-time tx-id))

  (store-index-meta [_ k v]
    (store-meta kv k v))

  (read-index-meta [_ k]
    (read-meta kv k)))

;; Query

(def ^:const default-doc-cache-size 10240)

(defrecord DocEntity [kv eid content-hash]
  crux.db/Entity
  (attr-val [this ident]
    (-> (lru-named-cache (:state kv) ::doc-cache default-doc-cache-size)
        (lru-cache-compute-if-absent
         content-hash
         #(nippy/fast-thaw (.array ^ByteBuffer (get (docs kv [%]) %))))
        (get ident)))
  (->id [this]
    eid))

(defrecord DocDatasource [kv business-time transact-time]
  crux.db/Datasource
  (entities [this]
    (for [[_ entity-map] (all-entities kv business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv))))

  (entities-for-attribute-value [this ident min-v max-v]
    (for [[_ entity-map] (entities-by-attribute-values-at kv ident [[min-v max-v]] business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv)))))
