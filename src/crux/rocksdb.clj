(ns crux.rocksdb
  (:require [byte-streams :as bs]
            [crux.kv-store :refer :all])
  (:import [org.rocksdb Options ReadOptions RocksDB RocksIterator Slice]
           [java.util Arrays]))

(defn- -get [^RocksDB db k]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (when (and (.isValid i) (bs/bytes= (.key i) k))
        (.value i))
      (finally
        (.close i)))))

(defn rocks-iterator->seq [^RocksIterator i & [pred]]
  (lazy-seq
   (when (and (.isValid i) (or (not pred) (pred (.key i))))
     (cons (conj [(.key i) (.value i)])
           (do (.next i)
               (rocks-iterator->seq i pred))))))

(defn- -seek-and-iterate
  "TODO, improve by getting prefix-same-as-start to work, so we don't
  need an upper-bound."
  [^RocksDB db k #^bytes upper-bound]
  (let [read-options (ReadOptions.)
        i ^RocksIterator (.newIterator db (.setIterateUpperBound read-options (Slice. upper-bound)))]
    (try
      (.seek i k)
      (doall (rocks-iterator->seq i))
      (finally
        (.close i)))))

(defn- -seek-and-iterate-bounded
  [^RocksDB db #^bytes k]
  (let [read-options (doto (ReadOptions.) (.setPrefixSameAsStart true))
        i ^RocksIterator (.newIterator db read-options)
        array-length (alength k)]
    (try
      (.seek i k)
      (doall (rocks-iterator->seq i #(Arrays/equals k (Arrays/copyOfRange #^bytes % 0 array-length))))
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

  (seek-and-iterate [{:keys [^RocksDB db]} k upper-bound]
    (-seek-and-iterate db k upper-bound))

  (seek-and-iterate-bounded [{:keys [^RocksDB db]} k]
    (-seek-and-iterate-bounded db k))

  (store [{:keys [^RocksDB db]} k v]
    (.put db k v))

  (merge! [{:keys [^RocksDB db]} k v]
    (.merge db k v))

  (close [{:keys [^RocksDB db]}]
    (.close db))

  (destroy [this]
    (org.rocksdb.RocksDB/destroyDB (db-path db-name) (org.rocksdb.Options.))))

(defn crux-rocks-kv [db-name]
  (map->CruxRocksKv {:db-name db-name}))
