(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all])
  (:import [org.rocksdb RocksDB Options]))

(def key-entity-id (long->bytes 3))

;; The single, unconfigurable schema to rule them all..
(def schema {:foo 1
             :tar 2})

(defn- make-key
  ([k]
   (make-key k (java.util.Date.)))
  ([k ts]
   (-> (java.nio.ByteBuffer/allocate 12)
       (.putInt (schema k))
       (.putLong (.getTime ts))
       (.array))))

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

(defn -put
  ([db k v]
   (-put db k v (java.util.Date.)))
  ([db k v ts]
   (.put db (make-key k ts) (->bytes v))))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (.merge db key-entity-id (long->bytes 1))
    (bytes->long (.get db key-entity-id))))

(defn -get-at
  ([c k] (-get-at c k (java.util.Date.)))
  ([c k ts]
   (let [i (.newIterator c)
         k (make-key k ts)]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (= (take 8 k)
                                  (take 8 (.key i))))
         (bytes-> (.value i)))
       (finally
         (.close i))))))

(defn rocks-iterator->seq [i]
  (lazy-seq
   (when (.isValid i)
     (let [k (.key i)
           v (.value i)]
       (.next i)
       (cons [(bytes-> k) (bytes-> v)] (rocks-iterator->seq i))))))

(comment
  (def c (open-db "sd3"))
  (.close c)
  (defn print-all []
    (let [i (.newIterator c)]
      (try
        (.seekToFirst i)
        (doseq [v (rocks-iterator->seq i)]
          (println v))
        (finally
          (.close i))))))
