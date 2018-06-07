(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.doc.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.io Closeable]
           [java.util Date LinkedHashMap]
           [java.util.function Function]))

(set! *unchecked-math* :warn-on-boxed)

;; Utils

(defn- all-key-values-in-prefix [snapshot ^bytes prefix]
  (with-open [i (ks/new-iterator snapshot)]
    (loop [acc (transient [])
           k (ks/-seek i prefix)]
      (if (and k (bu/bytes=? prefix k))
        (recur (conj! acc [k (ks/-value i)]) (ks/-next i))
        (persistent! acc)))))

;; Docs

(defn doc-keys-by-attribute-values [snapshot k vs]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [v vs]
           (if (vector? v)
             (let [[min-v max-v] v]
               (when max-v
                 (assert (not (neg? (compare max-v min-v)))))
               [(idx/encode-attribute+value-prefix-key k (or min-v idx/empty-byte-array))
                (idx/encode-attribute+value-prefix-key k (or max-v idx/empty-byte-array))])
             (let [seek-k (idx/encode-attribute+value-prefix-key k (or v idx/empty-byte-array))]
               [seek-k seek-k])))
         (sort-by first bu/bytes-comparator)
         (reduce
          (fn [acc [min-seek-k ^bytes max-seek-k]]
            (loop [k (ks/-seek i min-seek-k)
                   acc acc]
              (if (and k (not (neg? (bu/compare-bytes max-seek-k k (alength max-seek-k)))))
                (recur (ks/-next i)
                       (->> (idx/decode-attribute+value+content-hash-key->content-hash k)
                            (conj acc)))
                acc)))
          #{}))))

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

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [seek-k (->> (for [entity entities]
                             (idx/encode-entity+bt+tt-prefix-key
                              entity
                              business-time
                              transact-time))
                           (sort bu/bytes-comparator))
               :let [entity-map (loop [k (ks/-seek i seek-k)]
                                  (when (and k (bu/bytes=? seek-k entity-prefix-size k))
                                    (let [v (ks/-value i)
                                          entity-map (-> (idx/decode-entity+bt+tt+tx-id-key k)
                                                         (enrich-entity-map v))]
                                      (if (<= (compare (:tt entity-map) transact-time) 0)
                                        (when-not (bu/bytes=? idx/nil-id-bytes v)
                                          entity-map)
                                        (recur (ks/-next i))))))]
               :when entity-map]
           [(:eid entity-map) entity-map])
         (into {}))))


(defn eids-by-content-hashes [snapshot content-hashes]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [content-hash content-hashes
               :let [content-hash (idx/new-id content-hash)]]
           [(idx/encode-content-hash-prefix-key content-hash)
            content-hash])
         (into (sorted-map-by bu/bytes-comparator))
         (reduce-kv
          (fn [acc seek-k content-hash]
            (loop [k (ks/-seek i seek-k)
                   acc acc]
              (if (and k (bu/bytes=? seek-k k))
                (recur (ks/-next i)
                       (update acc
                               content-hash
                               conj
                               (idx/decode-content-hash+entity-key->entity k)))
                acc)))
          {}))))

(defn entities-by-attribute-values-at [snapshot k vs business-time transact-time]
  (->> (for [[content-hash eids] (->> (doc-keys-by-attribute-values snapshot k vs)
                                      (eids-by-content-hashes snapshot))
             [eid entity-map] (entities-at snapshot eids business-time transact-time)
             :when (= content-hash (:content-hash entity-map))]
         [eid entity-map])
       (into {})))

(defn all-entities [snapshot business-time transact-time]
  (let [eids (->> (all-key-values-in-prefix snapshot (idx/encode-entity+bt+tt-prefix-key))
                  (map (comp :eid idx/decode-entity+bt+tt+tx-id-key first)))]
    (entities-at snapshot eids business-time transact-time)))

(defn entity-histories [snapshot entities]
  (->> (for [seek-k (->> (for [entity entities]
                           (idx/encode-entity+bt+tt-prefix-key entity))
                         (sort bu/bytes-comparator))
             :let [[entity-map :as history] (for [[k v] (all-key-values-in-prefix snapshot seek-k)]
                                              (-> (idx/decode-entity+bt+tt+tx-id-key k)
                                                  (enrich-entity-map v)))]
             :when entity-map]
         {(:eid entity-map) history})
       (into {})))


;; Caching

(defn- lru-cache [^long size]
  (proxy [LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [_]
      (> (count this) size))))

(defn- lru-cache-compute-if-absent [^LinkedHashMap cache k f]
  (.computeIfAbsent cache k (reify Function
                              (apply [_ k]
                                (f k)))))

(defn- lru-named-cache [state cache-name cache-size]
  (get (swap! state
              update
              cache-name
              (fn [cache]
                (or cache (lru-cache cache-size))))
       cache-name))

(defrecord CachedObjectStore [^LinkedHashMap cache object-store]
  db/ObjectStore
  (get-objects [this ks]
    (->> (for [k ks]
           [k (lru-cache-compute-if-absent
               cache
               k
               #(get (db/get-objects object-store [%]) %))])
         (into {})))
  (put-objects [this kvs]
    (db/put-objects object-store kvs))
  (delete-objects [this ks]
    (doseq [k ks]
      (.remove cache k))
    (db/delete-objects object-store ks))

  Closeable
  (close [_]))

;; Query

(defrecord DocEntity [object-store eid content-hash bt]
  db/Entity
  (attr-val [this ident]
    (get (db/->map this) ident))
  (->id [this]
    (db/attr-val this :crux.kv/id))
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
    (for [[_ entity-map] (all-entities query-context business-time transact-time)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entities-for-attribute-value [this query-context ident min-v max-v]
    (for [[_ entity-map] (entities-by-attribute-values-at query-context ident [[min-v max-v]] business-time transact-time)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entity-history [this query-context eid]
    (for [entity-map (get (entity-histories query-context [eid]) eid)]
      (map->DocEntity (assoc entity-map :object-store object-store))))

  (entity [this query-context eid]
    (when-let [entity-map (get (entities-at query-context [eid] business-time transact-time) eid)]
      (map->DocEntity (assoc entity-map :object-store object-store)))))

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException. (str "Timed out waiting for: " transact-time)))))))

(def ^:const default-doc-cache-size 10240)

(defn- new-cached-object-store [kv cache-size]
  (->CachedObjectStore (lru-named-cache (:state kv)::doc-cache cache-size)
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
