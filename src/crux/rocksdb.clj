(ns crux.rocksdb
  (:require [clojure.java.io :as io]
            [crux.kv-store :refer :all])
  (:import java.io.Closeable
           [org.rocksdb Checkpoint Options ReadOptions RocksDB RocksIterator WriteBatch WriteOptions]))

(defn- iterator->kv [^RocksIterator i]
  (when (.isValid i)
    [(.key i) (.value i)]))

(defn- ^Closeable rocks-iterator [{:keys [^RocksDB db ^ReadOptions vanilla-read-options]}]
  (let [i (.newIterator db vanilla-read-options)]
    (reify
      KvIterator
      (-seek [this k]
        (.seek i k)
        (iterator->kv i))
      (-next [this]
        (.next i)
        (iterator->kv i))
      Closeable
      (close [this]
        (.close i)))))

(defrecord CruxRocksKv [db-dir]
  CruxKvStore
  (open [this]
    (RocksDB/loadLibrary)
    (let [opts (doto (Options.)
                 (.setCreateIfMissing true))
          db (try
               (RocksDB/open opts (.getAbsolutePath (io/file db-dir)))
               (catch Throwable t
                 (.close opts)
                 (throw t)))]
      (assoc this :db db :options opts :vanilla-read-options (ReadOptions.))))

  (iterate-with [this f]
    (with-open [i (rocks-iterator this)]
      (f i)))

  (store [{:keys [^RocksDB db]} k v]
    (.put db k v))

  (store-all! [{:keys [^RocksDB db]} kvs]
    (with-open [wb (WriteBatch.)
                wo (WriteOptions.)]
      (doseq [[k v] kvs]
        (.put wb k v))
      (.write db wo wb)))

  (destroy [this]
    (with-open [options (Options.)]
      (RocksDB/destroyDB (.getAbsolutePath (io/file db-dir)) options)))

  (backup [{:keys [^RocksDB db]} dir]
    (.createCheckpoint (Checkpoint/create db) (.getAbsolutePath (io/file dir))))

  Closeable
  (close [{:keys [^RocksDB db ^Options options ^ReadOptions vanilla-read-options]}]
    (.close db)
    (.close options)
    (.close vanilla-read-options)))
