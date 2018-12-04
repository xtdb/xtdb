(ns crux.kv.rocksdb
  "RocksDB KV backend for Crux.

  Requires org.rocksdb/rocksdbjni on the classpath."
  (:require [clojure.java.io :as io]
            [crux.kv :as kv])
  (:import java.io.Closeable
           clojure.lang.MapEntry
           [org.rocksdb Checkpoint CompressionType Options ReadOptions
            RocksDB RocksIterator WriteBatch WriteOptions]))

(set! *unchecked-math* :warn-on-boxed)

(defn- iterator->key [^RocksIterator i]
  (when (.isValid i)
    (.key i)))

(defrecord RocksKvIterator [^RocksIterator i]
  kv/KvIterator
  (seek [this k]
    (.seek i k)
    (iterator->key i))

  (next [this]
    (.next i)
    (iterator->key i))

  (value [this]
    (.value i))

  (kv/refresh [this]
    ;; TODO: https://github.com/facebook/rocksdb/pull/3465
    this)

  Closeable
  (close [this]
    (.close i)))

(defrecord RocksKvSnapshot [^RocksDB db ^ReadOptions read-options snapshot]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksKvIterator (.newIterator db read-options)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKv [db-dir]
  kv/KvStore
  (open [this]
    (RocksDB/loadLibrary)
    (let [opts (doto (Options.)
                 (.setCompressionType CompressionType/LZ4_COMPRESSION)
                 (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION)
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
