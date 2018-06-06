(ns crux.doc
  (:require [crux.byte-utils :as bu]
            [crux.doc.index :as idx]
            [crux.kv-store :as ks]
            [crux.kv-store-utils :as kvu]
            [crux.db :as db]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.util Date LinkedHashMap]
           [java.util.function Function]))

(set! *unchecked-math* :warn-on-boxed)

;; Utils

(defn- all-key-values-in-prefix [snapshot ^bytes prefix]
  (with-open [i (ks/new-iterator snapshot)]
    (loop [[k v] (ks/-seek i prefix)
           acc []]
      (if (and k (bu/bytes=? prefix k))
        (recur (ks/-next i) (conj acc [k v]))
        acc))))

;; Docs

(defn all-doc-keys [snapshot]
  (->> (all-key-values-in-prefix snapshot (idx/encode-doc-prefix-key))
       (map (comp idx/->Id idx/decode-doc-key first))
       (set)))

(defn docs [snapshot ks]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [seek-k (->> (map (comp idx/encode-doc-key idx/id->bytes) ks)
                           (sort bu/bytes-comparator))
               :let [[k v] (ks/-seek i seek-k)]
               :when (and k (bu/bytes=? seek-k k))]
           [(idx/->Id (idx/decode-doc-key k))
            (ByteBuffer/wrap v)])
         (into {}))))

(defn doc-keys-by-attribute-values [snapshot k vs]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [v vs]
           (if (vector? v)
             (let [[min-v max-v] v]
               (when max-v
                 (assert (not (neg? (compare max-v min-v)))))
               [(idx/encode-attribute+value-prefix-key (idx/id->bytes k) (idx/value->bytes (or min-v idx/empty-byte-array)))
                (idx/encode-attribute+value-prefix-key (idx/id->bytes k) (idx/value->bytes (or max-v idx/empty-byte-array)))])
             (let [seek-k (idx/encode-attribute+value-prefix-key (idx/id->bytes k) (idx/value->bytes (or v idx/empty-byte-array)))]
               [seek-k seek-k])))
         (sort-by first bu/bytes-comparator)
         (reduce
          (fn [acc [min-seek-k ^bytes max-seek-k]]
            (loop [[k v] (ks/-seek i min-seek-k)
                   acc acc]
              (if (and k (not (neg? (bu/compare-bytes max-seek-k k (alength max-seek-k)))))
                (recur (ks/-next i)
                       (->> (idx/decode-attribute+value+content-hash-key->content-hash k)
                            (idx/->Id)
                            (conj acc)))
                acc)))
          #{}))))

(defn- normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn store-doc [kv content-hash doc]
  (let [content-hash (idx/id->bytes content-hash)
        existing-doc (with-open [snapshot (ks/new-snapshot kv)]
                       (get (docs snapshot [content-hash]) (idx/->Id content-hash)))]
    (cond
      (and doc (nil? existing-doc))
      (ks/store kv (cons
                    [(idx/encode-doc-key content-hash)
                     (nippy/fast-freeze doc)]
                    (for [[k v] doc
                          v (normalize-value v)]
                      [(idx/encode-attribute+value+content-hash-key (idx/id->bytes k) (idx/value->bytes v) content-hash)
                       idx/empty-byte-array])))

      (and (nil? doc) existing-doc)
      (ks/delete kv (cons
                     (idx/encode-doc-key content-hash)
                     (for [[k v] (nippy/fast-thaw (.array ^ByteBuffer existing-doc))
                           v (normalize-value v)]
                       (idx/encode-attribute+value+content-hash-key (idx/id->bytes k) (idx/value->bytes v) content-hash)))))))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(idx/encode-meta-key (idx/id->bytes k))
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (some->> ^bytes (kvu/value kv (idx/encode-meta-key (idx/id->bytes k)))
           nippy/fast-thaw))

;; Txs Read

(defn- enrich-entity-map [entity-map content-hash]
  (-> entity-map
      (assoc :content-hash (some-> content-hash not-empty idx/->Id))
      (update :eid idx/->Id)))

(def ^:private ^:const entity-prefix-size (+ Short/BYTES idx/id-size))

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [seek-k (->> (for [entity entities]
                             (idx/encode-entity+bt+tt-prefix-key
                              (idx/id->bytes entity)
                              business-time
                              transact-time))
                           (sort bu/bytes-comparator))
               :let [entity-map (loop [[k v] (ks/-seek i seek-k)]
                                  (when (and k (bu/bytes=? seek-k entity-prefix-size k))
                                    (let [entity-map (-> (idx/decode-entity+bt+tt+tx-id-key k)
                                                         (enrich-entity-map v))]
                                      (if (<= (compare (:tt entity-map) transact-time) 0)
                                        (when-not (empty? v)
                                          entity-map)
                                        (recur (ks/-next i))))))]
               :when entity-map]
           [(:eid entity-map) entity-map])
         (into {}))))


(defn eids-by-content-hashes [snapshot content-hashes]
  (with-open [i (ks/new-iterator snapshot)]
    (->> (for [content-hash content-hashes
               :let [content-hash (idx/id->bytes content-hash)]]
           [(idx/encode-content-hash-prefix-key content-hash)
            (idx/->Id content-hash)])
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
                               (idx/->Id (idx/decode-content-hash+entity-key->entity k))))
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
                  (map (comp idx/->Id :eid idx/decode-entity+bt+tt+tx-id-key first)))]
    (entities-at snapshot eids business-time transact-time)))

(defn entity-histories [snapshot entities]
  (->> (for [seek-k (->> (for [entity entities]
                           (idx/encode-entity+bt+tt-prefix-key (idx/id->bytes entity)))
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

;; Query

(def ^:const default-doc-cache-size 10240)

(defrecord DocEntity [kv query-context eid content-hash]
  db/Entity
  (attr-val [this ident]
    (-> (lru-named-cache (:state kv) ::doc-cache default-doc-cache-size)
        (lru-cache-compute-if-absent
         content-hash
         #(nippy/fast-thaw (.array ^ByteBuffer (get (docs query-context [%]) %))))
        (get ident)))
  (->id [this]
    (db/attr-val this :crux.kv/id))
  (eq? [this that]
    (= eid (:eid that))))

(defrecord DocDatasource [kv business-time transact-time]
  db/Datasource
  (new-query-context [this]
    (ks/new-snapshot kv))

  (entities [this query-context]
    (for [[_ entity-map] (all-entities query-context business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv :query-context query-context))))

  (entities-for-attribute-value [this query-context ident min-v max-v]
    (for [[_ entity-map] (entities-by-attribute-values-at query-context ident [[min-v max-v]] business-time transact-time)]
      (map->DocEntity (assoc entity-map :kv kv :query-context query-context)))))

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException. (str "Timed out waiting for: " transact-time)))))))

(defn db
  ([kv]
   (db kv (Date.)))
  ([kv business-time]
   (->DocDatasource kv business-time business-time))
  ([kv business-time transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout)
   (->DocDatasource kv business-time transact-time)))
