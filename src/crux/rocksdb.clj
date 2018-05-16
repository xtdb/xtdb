(ns crux.rocksdb
  (:require [clojure.java.io :as io]
            [crux.byte-utils :as bu]
            [crux.kv-store :refer :all]
            [crux.io :as cio])
  (:import clojure.lang.IReduceInit
           java.io.Closeable
           [org.rocksdb
            BackupEngine BackupableDBOptions
            Env Options ReadOptions RestoreOptions
            RocksDB RocksIterator
            WriteBatch WriteOptions]))

;; Todo move to kv-store
(defprotocol KvIterator
  (-seek [this k])
  (next [this]))

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
      (next [this]
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

  (value [this seek-k]
    (with-open [i (rocks-iterator this)]
      (when-let [[k v] (-seek i seek-k)]
        (when (zero? (bu/compare-bytes seek-k k Integer/MAX_VALUE))
          v))))

  (seek [this k]
    (with-open [i (rocks-iterator this)]
      (-seek i k)))

  (seek-first [this prefix-pred key-pred seek-k]
    (with-open [i (rocks-iterator this)]
      (loop [[k v :as kv] (-seek i seek-k)]
        (when (and k (prefix-pred k))
          (if (key-pred k)
            kv
            (recur (next i)))))))

  (seek-and-iterate [rocks-kv key-pred seek-k]
    (reify
      IReduceInit
      (reduce [this f init]
        (with-open [i (rocks-iterator rocks-kv)]
          (loop [init init
                 [k v :as kv] (-seek i seek-k)]
            (if (and k (key-pred k))
              (let [result (f init kv)]
                (if (reduced? result)
                  @result
                  (recur result (next i))))
              init))))))

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
    (let [tmpdir (cio/create-tmpdir "rocksdb-backup")]
      (try
        (let [file (io/file dir)]
          (.mkdirs file)
          (with-open [backup-options (BackupableDBOptions. (.getAbsolutePath tmpdir))
                      restore-options (RestoreOptions. false)
                      engine (BackupEngine/open (Env/getDefault) backup-options)]
            (.createNewBackup engine db)
            (.restoreDbFromLatestBackup engine (.getAbsolutePath file) (.getAbsolutePath file) restore-options)))
        (finally
          (cio/delete-dir tmpdir)))))

  Closeable
  (close [{:keys [^RocksDB db ^Options options ^ReadOptions vanilla-read-options]}]
    (.close db)
    (.close options)
    (.close vanilla-read-options)))
