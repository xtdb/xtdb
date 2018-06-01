(ns crux.doc
  (:require [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.kv-store-utils :as kvu]
            [crux.db :as db]
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

(defn- encode-doc-prefix-key ^bytes []
  (-> (ByteBuffer/allocate (+ Short/BYTES))
      (.putShort content-hash->doc-index-id)
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

(defn- encode-entity+bt+tt+tx-id-key ^bytes [^bytes eid ^Date business-time ^Date transact-time ^Long tx-id]
  (assert (= id-size (alength eid)))
  (cond-> (ByteBuffer/allocate (cond-> (+ Short/BYTES id-size Long/BYTES Long/BYTES)
                                 tx-id (+ Long/BYTES)))
    true (-> (.putShort entity+bt+tt+tx-id->content-hash-index-id)
             (.put eid)
             (.putLong (date->reverse-time-ms business-time))
             (.putLong (date->reverse-time-ms transact-time)))
    tx-id (.putLong tx-id)
    true (.array)))

(defn- encode-entity+bt+tt-prefix-key
  (^bytes []
   (-> (ByteBuffer/allocate Short/BYTES)
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.array)))
  (^bytes [^bytes eid]
   (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.put eid)
       (.array)))
  (^bytes [^bytes eid ^Date business-time ^Date transact-time]
   (encode-entity+bt+tt+tx-id-key eid business-time transact-time nil)))

(defn- decode-entity+bt+tt+tx-id-key [^bytes key]
  (assert (= (+ Short/BYTES id-size Long/BYTES Long/BYTES Long/BYTES)) (alength key))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id (.getShort buffer)))
    {:eid (doto (byte-array id-size)
            (->> (.get buffer)))
     :bt (reverse-time-ms->date (.getLong buffer))
     :tt (reverse-time-ms->date (.getLong buffer))
     :tx-id (.getLong buffer)}))

(defn- all-key-values-in-prefix [kv ^bytes prefix]
  (ks/iterate-with
   kv
   (fn [i]
     (loop [[k v] (ks/-seek i prefix)
            acc []]
       (if (and k (bu/bytes=? prefix k))
         (recur (ks/-next i) (conj acc [k v]))
         acc)))))

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
  (->> (all-key-values-in-prefix kv (encode-doc-prefix-key))
       (map (comp ->Id decode-doc-key first))
       (set)))

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

(defn- normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn store-doc [kv content-hash doc]
  (let [content-hash-bytes (id->bytes content-hash)
        content-hash (->Id content-hash-bytes)
        existing-doc (get (docs kv [content-hash]) content-hash)]
    (cond
      (and doc (nil? existing-doc))
      (ks/store kv (cons
                    [(encode-doc-key content-hash-bytes)
                     (nippy/fast-freeze doc)]
                    (for [[k v] doc
                          v (normalize-value v)]
                      [(encode-attribute+value+content-hash-key k v content-hash-bytes)
                       empty-byte-array])))

      (and (nil? doc) existing-doc)
      (ks/delete kv (cons
                     (encode-doc-key content-hash-bytes)
                     (for [[k v] (nippy/fast-thaw (.array ^ByteBuffer existing-doc))
                           v (normalize-value v)]
                       (encode-attribute+value+content-hash-key k v content-hash-bytes)))))))

;; Txs Read

(defn- enrich-entity-map [entity-map content-hash]
  (-> entity-map
      (assoc :content-hash (->Id content-hash))
      (update :eid ->Id)))

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
                                       (let [entity-map (-> (decode-entity+bt+tt+tx-id-key k)
                                                            (enrich-entity-map v))]
                                         (if (<= (compare (:tt entity-map) transact-time) 0)
                                           entity-map
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
  (->> (for [[content-hash eids] (->> (doc-keys-by-attribute-values kv k vs)
                                      (eids-by-content-hashes kv))
             [eid entity-map] (entities-at kv eids business-time transact-time)
             :when (= content-hash (:content-hash entity-map))]
         [eid entity-map])
       (into {})))

(defn all-entities [kv business-time transact-time]
  (let [eids (->> (all-key-values-in-prefix kv (encode-entity+bt+tt-prefix-key))
                  (map (comp ->Id :eid decode-entity+bt+tt+tx-id-key first)))]
    (entities-at kv eids business-time transact-time)))

(defn entity-histories [kv entities]
  (->> (for [seek-k (->> (for [entity entities]
                           (encode-entity+bt+tt-prefix-key (id->bytes entity)))
                         (sort bu/bytes-comparator))
             :let [[entity-map :as history] (for [[k v] (all-key-values-in-prefix kv seek-k)]
                                              (-> (decode-entity+bt+tt+tx-id-key k)
                                                  (enrich-entity-map v)))]
             :when entity-map]
         {(:eid entity-map) history})
       (into {})))

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

(s/def ::evict-op (s/cat :op #{:crux.tx/evict}
                         :id ::id))

(s/def ::tx-op (s/and (s/or :put ::put-op
                            :delete ::delete-op
                            :cas ::cas-op)
                      (s/conformer (comp vec vals second))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind vector?))

(defmulti tx-command (fn [kv tx-log [op] transact-time tx-id] op))

(defmethod tx-command :crux.tx/put [kv tx-log [op k v business-time] transact-time tx-id]
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

(defmethod tx-command :crux.tx/delete [kv tx-log [op k business-time] transact-time tx-id]
  (let [eid (id->bytes k)
        business-time (or business-time transact-time)]
    [[(encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      empty-byte-array]]))

(defmethod tx-command :crux.tx/cas [kv tx-log [op k old-v new-v business-time] transact-time tx-id]
  (let [eid (id->bytes k)
        business-time (or business-time transact-time)
        old-content-hash (-> (entities-at kv [eid] business-time transact-time)
                             (get (->Id eid))
                             :content-hash)
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

(defmethod tx-command :crux.tx/evict [kv tx-log [op k business-time] transact-time tx-id]
  (let [eid (id->bytes k)
        business-time (or business-time transact-time)]
    (when tx-log
      (doseq [{:keys [content-hash]} (get (entity-histories kv [eid]) (->Id eid))]
        (db/submit-doc tx-log (str content-hash) nil)))
    [[(encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      empty-byte-array]]))

(defn store-tx [kv tx-log tx-ops tx-time tx-id]
  (->> (for [tx-op tx-ops]
         (tx-command kv tx-log tx-op tx-time tx-id))
       (reduce into {})
       (ks/store kv)))

(defrecord DocIndexer [kv tx-log]
  db/Indexer
  (index-doc [_ content-hash doc]
    (store-doc kv content-hash doc))

  (index-tx [_ tx-ops tx-time tx-id]
    (store-tx kv tx-log tx-ops tx-time tx-id))

  (store-index-meta [_ k v]
    (store-meta kv k v))

  (read-index-meta [_ k]
    (read-meta kv k)))

(defn conform-tx-ops [tx-ops]
  (let [conformed-ops (s/conform ::tx-ops tx-ops)]
    (if (s/invalid? conformed-ops)
      (throw (ex-info "Invalid input" (s/explain-data ::tx-ops tx-ops)))
      conformed-ops)))

(defn tx-ops->docs [tx-ops]
  (for [tx-op tx-ops
        doc (filter map? tx-op)]
    doc))

(defrecord DocTxLog [kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (->DocIndexer kv this) content-hash doc))

  (submit-tx [this tx-ops]
    (let [transact-time (Date.)
          tx-id (.getTime transact-time)
          conformed-tx-ops (conform-tx-ops tx-ops)]
      (doseq [doc (tx-ops->docs tx-ops)]
        (db/submit-doc this (str (doc->content-hash doc)) doc))
      (db/index-tx (->DocIndexer kv this) conformed-tx-ops transact-time tx-id)
      (delay {:tx-id tx-id
              :transact-time transact-time}))))

;; Query

(def ^:const default-doc-cache-size 10240)

(defrecord DocEntity [kv eid content-hash]
  db/Entity
  (attr-val [this ident]
    (-> (lru-named-cache (:state kv) ::doc-cache default-doc-cache-size)
        (lru-cache-compute-if-absent
         content-hash
         #(nippy/fast-thaw (.array ^ByteBuffer (get (docs kv [%]) %))))
        (get ident)))
  (->id [this]
    eid))

(defrecord DocDatasource [kv business-time transact-time]
  db/Datasource
  (entities [this]
    (for [[_ entity-map] (all-entities kv business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv))))

  (entities-for-attribute-value [this ident min-v max-v]
    (for [[_ entity-map] (entities-by-attribute-values-at kv ident [[min-v max-v]] business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv)))))

(defn db
  ([kv]
   (let [now (Date.)]
     (db kv now now)))
  ([kv business-time]
   (db kv business-time (Date.)))
  ([kv business-time transact-time]
   (->DocDatasource kv business-time transact-time)))
