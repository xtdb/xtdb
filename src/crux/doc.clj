(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [crux.lru :as lru]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.io Closeable]
           [java.util Date]))

(set! *unchecked-math* :warn-on-boxed)

;; Utils

(defn- all-keys-in-prefix-seq [i prefix]
  (let [step (fn step [f-cons f-next]
               (lazy-seq
                (let [k (f-cons)]
                  (when (and k (bu/bytes=? prefix k))
                    (cons k (step f-next f-next))))))]
    (step #(ks/-seek i prefix) #(ks/-next i))))

(defn- all-key-values-in-prefix-seq [i prefix]
  (let [step (fn step [f-cons f-next]
               (lazy-seq
                (let [k (f-cons)]
                  (when (and k (bu/bytes=? prefix k))
                    (cons [k (ks/-value i)]
                          (step f-next f-next))))))]
    (step #(ks/-seek i prefix) #(ks/-next i))))

;; Docs

(defn- attribute-value+index-key->content-hashes [attr k max-v]
  (let [max-seek-k (idx/encode-attribute+value-prefix-key attr (or max-v idx/empty-byte-array))]
    (when (and k (not (neg? (bu/compare-bytes max-seek-k k (alength max-seek-k)))))
      [(idx/decode-attribute+value+content-hash-key->content-hash k)])))

(defrecord DocAttrbuteValueIndex [i attr max-v]
  db/Index
  (-seek-values [this k]
    (when-let [k (->> (or k idx/empty-byte-array)
                      (idx/encode-attribute+value-prefix-key attr)
                      (ks/-seek i))]
      (attribute-value+index-key->content-hashes attr k max-v)))
  (-next-values [this]
    (when-let [k (ks/-next i)]
      (attribute-value+index-key->content-hashes attr k max-v))))

(defn doc-keys-by-attribute-value [snapshot attr min-v max-v]
  (with-open [i (ks/new-iterator snapshot)]
    (let [index (->DocAttrbuteValueIndex i attr max-v)]
      (when-let [k (db/-seek-values index min-v)]
        (->> (repeatedly #(db/-next-values index))
             (take-while identity)
             (apply concat k)
             (vec))))))

(defn- normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn index-doc [kv content-hash doc]
  (ks/store kv (for [[k v] doc
                     v (normalize-value v)]
                 [(idx/encode-attribute+value+content-hash-key k v content-hash)
                  idx/empty-byte-array])))

(defn delete-doc-from-index [kv content-hash doc]
  (ks/delete kv (for [[k v] doc
                      v (normalize-value v)]
                  (idx/encode-attribute+value+content-hash-key k v content-hash))))

(defrecord DocObjectStore [kv]
  db/ObjectStore
  (get-objects [this ks]
    (with-open [snapshot (ks/new-snapshot kv)
                i (ks/new-iterator snapshot)]
      (->> (for [seek-k (->> (map idx/encode-doc-key ks)
                             (sort bu/bytes-comparator))
                 :let [k (ks/-seek i seek-k)]
                 :when (and k (bu/bytes=? seek-k k))]
             [(idx/decode-doc-key k)
            (nippy/fast-thaw (ks/-value i))])
           (into {}))))
  (put-objects [this kvs]
    (ks/store kv (for [[k v] kvs]
                   [(idx/encode-doc-key k)
                    (nippy/fast-freeze v)])))
  (delete-objects [this ks]
    (ks/delete kv (map idx/encode-doc-key ks)))

  Closeable
  (close [_]))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(idx/encode-meta-key k)
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (with-open [snapshot (ks/new-snapshot kv)
              i (ks/new-iterator snapshot)]
    (when-let [k (ks/-seek i (idx/encode-meta-key k))]
      (nippy/fast-thaw (ks/-value i)))))

;; Entities

(defn- enrich-entity-map [entity-map content-hash]
  (assoc entity-map :content-hash (some-> content-hash not-empty idx/new-id)))

(def ^:private ^:const entity-prefix-size (+ Short/BYTES idx/id-size))

(defn- entity-at [i entity business-time transact-time]
  (let [seek-k (idx/encode-entity+bt+tt-prefix-key
                entity
                business-time
                transact-time)]
    (loop [k (ks/-seek i seek-k)]
      (when (and k (bu/bytes=? seek-k entity-prefix-size k))
        (let [v (ks/-value i)
              entity-map (-> (idx/decode-entity+bt+tt+tx-id-key k)
                             (enrich-entity-map v))]
          (if (<= (compare (:tt entity-map) transact-time) 0)
            (when-not (bu/bytes=? idx/nil-id-bytes v)
              entity-map)
            (recur (ks/-next i))))))))

(defn- entities-at-seq [i entities business-time transact-time]
  (for [entity entities
        :let [entity-map (entity-at i entity business-time transact-time)]
        :when entity-map]
    entity-map))

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (vec (entities-at-seq i entities business-time transact-time))))

(defn- content-hash+eid-by-content-hashes-seq [i content-hashes]
  (for [content-hash content-hashes
        :let [content-hash (idx/new-id content-hash)
              seek-k (idx/encode-content-hash-prefix-key content-hash)]
        eid (->> (all-keys-in-prefix-seq i seek-k)
                 (map idx/decode-content-hash+entity-key->entity))]
    [content-hash eid]))

(defn content-hash+eid-by-content-hashes [snapshot content-hashes]
  (with-open [i (ks/new-iterator snapshot)]
    (vec (content-hash+eid-by-content-hashes-seq i content-hashes))))

(defn- entities-for-content-hashes-seq [ci ei business-time transact-time content-hashes]
  (for [content-hash+eids (->> content-hashes
                               (content-hash+eid-by-content-hashes-seq ci)
                               (partition-by first))
        :let [content-hash (ffirst content-hash+eids)
              eids (map second content-hash+eids)]
        entity-map (entities-at-seq ei eids business-time transact-time)
        :when (= content-hash (:content-hash entity-map))]
    entity-map))

(defrecord EntityAttributeValueVirtualIndex [doc-index ci ei business-time transact-time]
  db/Index
  (-seek-values [this k]
    (when-let [content-hashes (db/-seek-values doc-index k)]
      (entities-for-content-hashes-seq ci ei business-time transact-time content-hashes)))
  (-next-values [this]
    (when-let [content-hashes (db/-next-values doc-index)]
      (entities-for-content-hashes-seq ci ei business-time transact-time content-hashes))))

(defn- entities-by-attribute-value-at-seq [entity-index min-v]
  (let [step (fn step [f-cons f-next]
               (when-let [k (f-cons)]
                 (lazy-cat k (step f-next f-next))))]
    (step #(db/-seek-values entity-index min-v) #(db/-next-values entity-index))))

(defn entities-by-attribute-value-at [snapshot attr min-v max-v business-time transact-time]
  (with-open [di (ks/new-iterator snapshot)
              ci (ks/new-iterator snapshot)
              ei (ks/new-iterator snapshot)]
    (let [doc-index (->DocAttrbuteValueIndex di attr max-v)
          entity-index (->EntityAttributeValueVirtualIndex doc-index ci ei business-time transact-time)]
      (vec (entities-by-attribute-value-at-seq entity-index min-v)))))

(defn all-entities [snapshot business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [eids (->> (all-keys-in-prefix-seq i (idx/encode-entity+bt+tt-prefix-key))
                    (map (comp :eid idx/decode-entity+bt+tt+tx-id-key))
                    (distinct))]
      (entities-at snapshot eids business-time transact-time))))

(defn entity-history [snapshot entity]
  (with-open [i (ks/new-iterator snapshot)]
    (let [seek-k (idx/encode-entity+bt+tt-prefix-key entity)]
      (vec (for [[k v] (all-key-values-in-prefix-seq i seek-k)]
             (-> (idx/decode-entity+bt+tt+tx-id-key k)
                 (enrich-entity-map v)))))))

;; Caching

(defrecord CachedObjectStore [cache object-store]
  db/ObjectStore
  (get-objects [this ks]
    (->> (for [k ks]
           [k (lru/compute-if-absent
               cache
               k
               #(get (db/get-objects object-store [%]) %))])
         (into {})))
  (put-objects [this kvs]
    (db/put-objects object-store kvs))
  (delete-objects [this ks]
    (doseq [k ks]
      (lru/evict cache k))
    (db/delete-objects object-store ks))

  Closeable
  (close [_]))

;; Query

(defrecord DocEntity [object-store eid content-hash bt]
  db/Entity
  (attr-val [this ident]
    (get (db/->map this) ident))
  (->id [this]
    ;; TODO: we want to get rid of the need for :crux.kv/id
    (or (db/attr-val this :crux.kv/id) eid))
  (->map [this]
    (get (db/get-objects object-store [content-hash]) content-hash))
  (->business-time [this]
    bt)
  (eq? [this that]
    (= eid (:eid that))))

(defrecord DocCachedIterator [iterators i]
  ks/KvIterator
  (-seek [_ k]
    (ks/-seek i k))
  (-next [_]
    (ks/-next i))
  (-value [_]
    (ks/-value i))

  Closeable
  (close [_]
    (swap! iterators conj i)))

(defrecord DocSnapshot [^Closeable snapshot iterators]
  ks/KvSnapshot
  (new-iterator [_]
    (let [is @iterators]
      (if-let [i (first is)]
        (if (compare-and-set! iterators is (disj is i))
          (->DocCachedIterator iterators i)
          (recur))
        (->> (ks/new-iterator snapshot)
             (->DocCachedIterator iterators)))))

  Closeable
  (close [_]
    (doseq [^Closeable i @iterators]
      (.close i))
    (.close snapshot)))

(defrecord DocDatasource [kv object-store business-time transact-time]
  db/Datasource
  (new-query-context [this]
    (->DocSnapshot (ks/new-snapshot kv) (atom #{})))

  (entities [this query-context]
    (for [entity-map (all-entities query-context business-time transact-time)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entities-for-attribute-value [this query-context ident min-v max-v]
    (for [entity-map (entities-by-attribute-value-at query-context ident min-v max-v business-time transact-time)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entity-history [this query-context eid]
    (for [entity-map (entity-history query-context eid)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entity [this query-context eid]
    (when-let [entity-map (first (entities-at query-context [eid] business-time transact-time)) ]
      (map->DocEntity (assoc entity-map :object-store object-store)))))

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException. (str "Timed out waiting for: " transact-time)))))))

(def ^:const default-doc-cache-size 10240)

(defn- named-cache [state cache-name cache-size]
  (get (swap! state
              update
              cache-name
              (fn [cache]
                (or cache (lru/new-cache cache-size))))
       cache-name))

(defn- new-cached-object-store [kv cache-size]
  (->CachedObjectStore (named-cache (:state kv)::doc-cache cache-size)
                       (->DocObjectStore kv)))

(defn db
  ([kv]
   (db kv (Date.)))
  ([kv business-time]
   (->DocDatasource kv
                    (new-cached-object-store kv default-doc-cache-size)
                    business-time
                    (Date.)))
  ([kv business-time transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout)
   (->DocDatasource kv
                    (new-cached-object-store kv default-doc-cache-size)
                    business-time
                    transact-time)))
