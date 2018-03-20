(ns juxt.rocks
  (:require [taoensso.nippy :as nippy])
  (:import [org.rocksdb RocksDB Options]))

;; The single, unconfigurable schema to rule them all..
(def schema {:foo 1
             :tar 2})

(defn ->bytes [v]
  ;;(nippy/freeze v)
  (.getBytes v java.nio.charset.StandardCharsets/UTF_8))

(defn bytes-> [b]
  (String. b java.nio.charset.StandardCharsets/UTF_8))

(defn- make-key [k ts]
  (let [a-id (schema k)]
    (byte-array (mapcat seq [(.toByteArray (biginteger a-id)) (.toByteArray (biginteger (.getTime ts)))]))))

(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defn open-db [db-name]
  ;; Open database
  (RocksDB/loadLibrary)
  (let [opts (doto (Options.)
               (.setCreateIfMissing true))]
    (RocksDB/open opts (db-path db-name))))

(defn destroy-db [db-name]
  (org.rocksdb.RocksDB/destroyDB (db-path db-name) (org.rocksdb.Options.)))

(defn -put
  ([db k v]
   (-put db k v (java.util.Date.)))
  ([db k v ts]
   (.put db (make-key k ts) (->bytes v))))

(defn -get-at
  ([c k] (-get-at c k (java.util.Date.)))
  ([c k ts]
   (let [i (.newIterator c)]
     (try
       (.seekForPrev i (make-key k ts))
       (when (.isValid i)
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
  (defn print-all []
    (let [i (.newIterator c)]
      (try
        (.seekToFirst i)
        (doseq [v (rocks-iterator->seq i)]
          (println v))
        (finally
          (.close i))))))
