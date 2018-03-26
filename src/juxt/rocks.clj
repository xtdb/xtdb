(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all]
            [clojure.set])
  (:import [org.rocksdb RocksDB Options]))

(def temp-id -1)
(def key-entity-id 1)
(def key-index-eat 2)
(def key-index-attr 3)
(def key-index-aid->hash 4)

(defn- ident->hash [ident]
  (hash (str (namespace ident) (name ident))))

(defn- attr->key [ident]
  (-> (java.nio.ByteBuffer/allocate 8)
      (.putInt (int key-index-attr))
      (.putInt (ident->hash ident))
      (.array)))

(defn- attr-aid->key [aid]
  (-> (java.nio.ByteBuffer/allocate 12)
      (.putInt (int key-index-aid->hash))
      (.putLong aid)
      (.array)))

(defn- eat->key
  "Make a key, using <index-id><entity-id><attribute-id><timestamp>"
  ([eid aid]
   (eat->key eid aid (java.util.Date.)))
  ([eid aid ts]
   (-> (java.nio.ByteBuffer/allocate 28)
       (.putInt key-index-eat)
       (.putLong eid)
       (.putLong aid)
       (.putLong (or (and ts (.getTime ts)) 0))
       (.array))))

(defn- parse-key "Transform back the key byte-array" [k]
  (let [key-byte-buffer (java.nio.ByteBuffer/wrap k)]
    [(.getLong key-byte-buffer 4) ;; The entity ID
     (.getLong key-byte-buffer 12) ;; The attribute ID
     (java.util.Date. (.getLong key-byte-buffer 16))]))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (long->bytes key-entity-id)]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident]}]
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (.put db (attr->key ident) (long->bytes aid))
    ;; to go from aid -> k
    (.put db (attr-aid->key aid) (->bytes (if (namespace ident)
                                            (str (namespace ident) "/" (name ident))
                                            (name ident))))))

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

(defn- attr-aid->schema [db aid]
  (let [i (.newIterator db)
        k (attr-aid->key aid)]
    (try
      (.seek i k)
      (if (and (.isValid i)
               (= (bytes->int (byte-array (drop 4 (.key i)))) (bytes->int (byte-array (drop 4 k)))))
        (keyword (bytes-> (.value i)))
        (throw (IllegalArgumentException. (str "Unrecognised attribute: " aid))))
      (finally
        (.close i)))))

(defn -put
  "Put an attribute/value tuple against an entity ID. If the supplied
  entity ID is -1, then a new entity-id will be generated."
  ([db tx]
   (-put db tx (java.util.Date.)))
  ([db tx ts]
   (let [txs (if (map? tx)
               (for [[k v] (dissoc tx ::id)]
                 [(::id tx) k v])
               [tx])]
     (doseq [[eid k v] txs]
       (let [aid (attr-schema db k)
             eid (or (and (= temp-id eid) (next-entity-id db)) eid)]
         (.put db (eat->key eid aid ts) (->bytes v)))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         i (.newIterator db)
         k (eat->key eid aid ts)]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (= (take 20 k)
                                  (take 20 (.key i))))
         (bytes-> (.value i)))
       (finally
         (.close i))))))

(defn rocks-iterator->seq [i pred?]
  (lazy-seq
   (when (and (.isValid i) (or (not pred?) (pred? i)))
     (cons (conj [(.key i) (.value i)])
           (do (.next i)
               (rocks-iterator->seq i pred?))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (let [eid-key (-> (java.nio.ByteBuffer/allocate 12)
                     (.putInt key-index-eat)
                     (.putLong eid)
                     (.array))]
     (let [i (.newIterator db)]
       (try
         (.seek i eid-key)
         (into {}
               (for [[k v] (rocks-iterator->seq i (fn [i] (= (take 12 eid-key)
                                                             (take 12 (.key i)))))
                     :let [[eid aid _] (parse-key k)
                           attr-k (attr-aid->schema db aid)]]
                 [attr-k (bytes-> v)]))
         (finally
           (.close i)))))))

(defn all-keys [db]
  (let [i (.newIterator db)]
    (try
      (.seekToFirst i)
      (println "Keys in the DB:")
      (doseq [v (rocks-iterator->seq i nil)]
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
