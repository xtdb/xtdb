(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all]
            [clojure.set]
            [byte-streams :as bs]
            [gloss.core :as g]
            [gloss.io :refer [encode decode contiguous]]
            [juxt.rocksdb :as rocksdb])
  (:import [org.rocksdb RocksDB Options]))

(def temp-id -1)
(def key-entity-id 1)
(def key-index-eat 2)
(def key-index-attr 3)
(def key-index-aid->hash 4)

(def attr-types {::Long {:id 1
                         :->bytes long->bytes
                         :<-bytes bytes->long}
                 ::String {:id 2
                           :->bytes str->bytes
                           :<-bytes bytes->str}})

(def attr-types-by-id (into {} (map (juxt (comp :id val) val) attr-types)))

(defn- ident->hash [ident]
  (hash (str (namespace ident) (name ident))))

(defn- attr->key [ident]
  (-> (java.nio.ByteBuffer/allocate 8)
      (.putInt (int key-index-attr))
      (.putInt (ident->hash ident))
      (.array)))

(def key-attribute-schema-frame (g/compile-frame {:index-id :int16
                                                  :aid :uint32}))

(g/defcodec val-attribute-schema-frame (g/ordered-map
                                        :attr/type :int16
                                        :attr/ident (g/string :utf-8)))

(g/defcodec key-eid-frame (g/ordered-map :index-id :int32
                                         :eid :int64))

;; TODO revise this down, we don't need minus numbers, can halve the mofo
(g/defcodec key-eid-aid-ts-frame (g/ordered-map :index-id :int32
                                                :eid :int64
                                                :aid :int64
                                                :ts :int64))

(defn- attr-aid->key [aid]
  (->> {:index-id key-index-aid->hash ;; todo use an enum
        :aid aid}
       (encode key-attribute-schema-frame)
       (bs/to-byte-array)))

(defn- eat->key
  "Make a key, using <index-id><entity-id><attribute-id><timestamp>"
  ([eid aid]
   (eat->key eid aid (java.util.Date.)))
  ([eid aid ts]
   (->> {:index-id key-index-eat
         :eid eid
         :aid aid
         :ts (.getTime ts)}
        (encode key-eid-aid-ts-frame)
        (bs/to-byte-array))))

;; was 28 (int, long 4, long 4, long 4)

(->> {:index-id 0
      :eid 23
      :aid 12
      :ts (.getTime (java.util.Date.))}
     (encode key-eid-aid-ts-frame)
     (bs/to-byte-array)
     count)

(defn- parse-key "Transform back the key byte-array" [k]
  (let [key-byte-buffer (java.nio.ByteBuffer/wrap k)]
    [(.getLong key-byte-buffer 4) ;; The entity ID
     (.getLong key-byte-buffer 12) ;; The attribute ID
     (java.util.Date. (.getLong key-byte-buffer 16))]))

(defn- parse-attr "Transform attribute from the byte-array" [k]
  (let [bb (java.nio.ByteBuffer/wrap k)]
    [(.getInt 0)
     (.getString bb 4)]))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (long->bytes key-entity-id)]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (.put db (attr->key ident) (long->bytes aid))
    ;; to go from aid -> k
    (let [k (attr-aid->key aid)]
      (.put db k (let [stringified-k (if (namespace ident)
                                       (str (namespace ident) "/" (name ident))
                                       (name ident))]
                   (->> {:attr/type ((attr-types type) :id) ;; todo use an enum
                         :attr/ident stringified-k}
                        (encode val-attribute-schema-frame)
                        (bs/to-byte-array)))))
    aid))

(defn- attr-schema [db ident]
  (if-let [[_ v] (rocksdb/get db (attr->key ident))]
    (bytes->long v)
    (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [[k v ] (rocksdb/get db (attr-aid->key aid))]
    (update (decode val-attribute-schema-frame v) :attr/ident keyword)
    (throw (IllegalArgumentException. (str "Unrecognised attribute: " aid)))))

(defn -put
  "Put an attribute/value tuple against an entity ID. If the supplied
  entity ID is -1, then a new entity-id will be generated."
  ([db txs]
   (-put db txs (java.util.Date.)))
  ([db txs ts]
   (let [txs (if (map? txs)
               (for [[k v] (dissoc txs ::id)]
                 [(::id txs) k v])
               txs)]
     (doseq [[eid k v] txs]
       (let [aid (attr-schema db k)
             attr-schema (attr-aid->schema db aid)
             attr-schema-def (get attr-types-by-id (:attr/type attr-schema))
             eid (or (and (= temp-id eid) (next-entity-id db)) eid)]
         (.put db (eat->key eid aid ts) ((:->bytes attr-schema-def) v)))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         attr-schema (attr-aid->schema db aid)
         i (.newIterator db)
         k (eat->key eid aid ts)]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (= (take 20 k)
                                  (take 20 (.key i))))
         ((:<-bytes (get attr-types-by-id (:attr/type attr-schema))) (.value i)))
       (finally
         (.close i))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (into {}
         (for [[k v] (rocksdb/seek-and-iterate db (->> {:index-id key-index-eat
                                                        :eid eid}
                                                       (encode key-eid-frame)
                                                       (bs/to-byte-array)))
               :let [[eid aid _] (parse-key k)
                     attr-schema (attr-aid->schema db aid)]]
           ;; Todo, pull this out into a fn
           [(:attr/ident attr-schema) ((:<-bytes (get attr-types-by-id (:attr/type attr-schema))) v)]))))

(defn all-keys [db]
  (let [i (.newIterator db)]
    (try
      (.seekToFirst i)
      (println "Keys in the DB:")
      (doseq [v (rocksdb/rocks-iterator->seq i nil)]
        (println v))
      (finally
        (.close i)))))

(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defn open-db [db-name]
  ;; Open database
  (RocksDB/loadLibrary)
  (let [opts (doto (Options.)
               (.setCreateIfMissing true)
               (.setMergeOperatorName "uint64add"))]
    (RocksDB/open opts (db-path db-name))))

(defn destroy-db [db-name]
  (org.rocksdb.RocksDB/destroyDB (db-path db-name) (org.rocksdb.Options.)))

(comment
  (def c (open-db "repldb"))
  (.close c)
  ;; Print all keys:
  (all-keys db))
