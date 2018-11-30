(ns crux.rocksdb
  "RocksDB KV backend for Crux.

  Requires org.rocksdb/rocksdbjni on the classpath."
  (:require [clojure.java.io :as io]
            [crux.kv-store :as ks])
  (:import java.io.Closeable
           clojure.lang.MapEntry
           [org.rocksdb Checkpoint Options ReadOptions RocksDB RocksIterator WriteBatch WriteOptions]))

(set! *unchecked-math* :warn-on-boxed)

(defn- iterator->key [^RocksIterator i]
  (when (.isValid i)
    (.key i)))

(defrecord RocksKvIterator [^RocksIterator i]
  ks/KvIterator
  (seek [this k]
    (.seek i k)
    (iterator->key i))
  (next [this]
    (.next i)
    (iterator->key i))
  (value [this]
    (.value i))

  Closeable
  (close [this]
    (.close i)))

(defrecord RocksKvSnapshot [^RocksDB db ^ReadOptions read-options snapshot]
  ks/KvSnapshot
  (new-iterator [this]
    (->RocksKvIterator (.newIterator db read-options)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKv [db-dir]
  ks/KvStore
  (open [this]
    (RocksDB/loadLibrary)
    (let [opts (doto (Options.)
                 (.setCreateIfMissing true))
          db (try
               (RocksDB/open opts (.getAbsolutePath (doto (io/file db-dir)
                                                      (.mkdirs))))
               (catch Throwable t
                 (.close opts)
                 (throw t)))]
      (assoc this :db db :options opts :write-options (doto (WriteOptions.)
                                                        (.setDisableWAL true)))))

  (new-snapshot [{:keys [^RocksDB db]}]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvSnapshot db
                         (doto (ReadOptions.)
                           (.setSnapshot snapshot))
                         snapshot)))

  (store [{:keys [^RocksDB db ^WriteOptions write-options]} kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs]
        (.put wb k v))
      (.write db write-options wb)))

  (delete [{:keys [^RocksDB db ^WriteOptions write-options]} ks]
    (with-open [wb (WriteBatch.)]
      (doseq [k ks]
        (.delete wb k))
      (.write db write-options wb)))

  (backup [{:keys [^RocksDB db]} dir]
    (.createCheckpoint (Checkpoint/create db) (.getAbsolutePath (io/file dir))))

  (count-keys [{:keys [^RocksDB db]}]
    (-> (.getProperty db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [{:keys [^RocksDB db ^Options options ^WriteOptions write-options]}]
    (.close db)
    (.close options)
    (.close write-options)))
