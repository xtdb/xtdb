(ns crux.rocksdb
  (:require [byte-streams :as bs]
            [crux.kv :refer :all])
  (:import [org.rocksdb Options ReadOptions RocksDB Slice]))

(defn- -get [db k]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (when (and (.isValid i) (bs/bytes= (.key i) k))
        (.value i))
      (finally
        (.close i)))))

(defn rocks-iterator->seq [i]
  (lazy-seq
   (when (and (.isValid i))
     (cons (conj [(.key i) (.value i)])
           (do (.next i)
               (rocks-iterator->seq i))))))

(defn- -seek-and-iterate
  "TODO, improve by getting prefix-same-as-start to work, so we don't
  need an upper-bound."
  [db k upper-bound]
  (let [read-options (ReadOptions.)
        i (.newIterator db (.setIterateUpperBound read-options (Slice. upper-bound)))]
    (try
      (.seek i k)
      (doall (rocks-iterator->seq i))
      (finally
        (.close i)))))

(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defrecord CruxRocksKv [db-name]
  CruxKv
  (open [this]
    (RocksDB/loadLibrary)
    (let [db (let [opts (doto (Options.)
                          (.setCreateIfMissing true)
                          (.setMergeOperatorName "uint64add"))]
               (RocksDB/open opts (db-path db-name)))]
      (assoc this :db db)))

  (seek [{:keys [db]} k]
    (-get db k))

  (seek-and-iterate [{:keys [db]} k upper-bound]
    (-seek-and-iterate db k upper-bound))

  (store [{:keys [db]} k v]
    (.put db k v))

  (merge! [{:keys [db]} k v]
    (.merge db k v))

  (close [{:keys [db]}]
    (.close db))

  (destroy [this]
    (org.rocksdb.RocksDB/destroyDB (db-path db-name) (org.rocksdb.Options.))))

(defn crux-rocks-kv [db-name]
  (map->CruxRocksKv {:db-name db-name}))
