(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all]
            [clojure.set])
  (:import [org.rocksdb RocksDB Options]))

(def temp-id -1)
(def key-entity-id 1)
(def key-index-eat 2)
(def key-index-attr 3)
(def schema-attribute-by-ids (clojure.set/map-invert schema))

(defn- make-key
  "Make a key, using <index-id><entity-id><attribute-id><timestamp>"
  ([eid k]
   (make-key eid k (java.util.Date.)))
  ([eid k ts]
   (-> (java.nio.ByteBuffer/allocate 24)
       (.putInt key-index-eat)
       (.putLong eid)
       (.putInt (or (schema k) 0)) ;; TODO not sure why this is nil?
       (.putLong (or (and ts (.getTime ts)) 0))
       (.array))))

(defn- parse-key "Transform back the key byte-array" [k]
  (let [key-byte-buffer (java.nio.ByteBuffer/wrap k)]
    [(.getLong key-byte-buffer 4)
     (get schema-attribute-by-ids (.getInt key-byte-buffer 12))
     (java.util.Date. (.getLong key-byte-buffer 16))]))

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

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (long->bytes key-entity-id)]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn- ident->hash [ident]
  (hash (str (namespace ident) (name ident))))

(defn- attr->key [ident]
  (-> (java.nio.ByteBuffer/allocate 8)
      (.putInt (int key-index-attr))
      (.putInt (ident->hash ident))
      (.array)))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident]}]
  (.put db (attr->key ident) (long->bytes (next-entity-id db))))

(defn- attr-schema [db ident]
  (let [i (.newIterator db)
        k (attr->key ident)]
    (try
      (.seek i k)
      (if (and (.isValid i)
               ;; TODO understand and simplify
               (= (bytes->int (byte-array (drop 4 (.key i)))) (bytes->int (byte-array (drop 4 k)))))
        (bytes->long (.value i))
        (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident))))
      (finally
        (.close i)))))

(defn -put
  "Put an attribute/value tuple against an entity ID. If the supplied
  entity ID is -1, then a new entity-id will be generated."
  ([db eid k v]
   (-put db eid k v (java.util.Date.)))
  ([db eid k v ts]
   (let [aid (attr-schema db k)
         eid (or (and (= temp-id eid) (next-entity-id db)) eid)]
     (.put db (make-key eid aid ts) (->bytes v)))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         i (.newIterator db)
         k (make-key eid aid ts)]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (= (take 16 k)
                                  (take 16 (.key i))))
         (bytes-> (.value i)))
       (finally
         (.close i))))))

(defn rocks-iterator->seq [i]
  (lazy-seq
   (when (.isValid i)
     (let [k (.key i)
           v (.value i)]
       (cons (conj (parse-key k) v)
             (do (.next i)
                 (rocks-iterator->seq i)))))))

(defn entity "Return an entity. Currently iterates through a list of
  known schema attributes. Another approach for consideration is
  iterate over all keys for a given entity and build up the map as we
  go. Unclear currently what the pros/cons are."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (into {}
         (for [[k _] schema
               :let [v (-get-at db eid k ts)]]
           [k v]))))

(defn all-keys [db]
  (let [i (.newIterator db)]
    (try
      (.seekToFirst i)
      (println "Keys in the DB:")
      (doseq [v (rocks-iterator->seq i)]
        (println v))
      (finally
        (.close i)))))

(comment
  (def c (open-db "repldb"))
  (.close c)
  ;; Print all keys:
  (all-keys db))
