(ns crux.rocksdb
  (:require [crux.kv-store :refer :all]
            [crux.byte-utils :as bu])
  (:import [org.rocksdb Options ReadOptions RocksDB RocksIterator Slice]))

(defn- -get [^RocksDB db k]
  (with-open [i (.newIterator db)]
    (.seek i k)
    (when (and (.isValid i) (zero? (bu/compare-bytes (.key i) k Integer/MAX_VALUE)))
      (.value i))))

(defn rocks-iterator->seq
  ([i]
   (rocks-iterator->seq i (constantly true)))
  ([^RocksIterator i pred]
   (lazy-seq
    (when (and (.isValid i) (pred (.key i)))
      (cons [(.key i) (.value i)]
            (do (.next i)
                (rocks-iterator->seq i pred)))))))

(defn- -seek-and-iterate
  "TODO, improve by getting prefix-same-as-start to work, so we don't
  need an upper-bound."
  [^RocksDB db k #^bytes upper-bound]
  (with-open [read-options (ReadOptions.)
              i ^RocksIterator (.newIterator db (.setIterateUpperBound read-options (Slice. upper-bound)))]
    (.seek i k)
    (doall (rocks-iterator->seq i))))

(defn- -seek-and-iterate-bounded
  [^RocksDB db #^bytes k]
  (with-open [read-options (doto (ReadOptions.) (.setPrefixSameAsStart true))
              i ^RocksIterator (.newIterator db read-options)]
    (let [array-length (alength k)]
      (.seek i k)
      (doall (rocks-iterator->seq i #(zero? (bu/compare-bytes k % array-length)))))))

;; TODO: this should be configurable.
(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defrecord CruxRocksKv [db-name]
  CruxKv
  (open [this]
    (RocksDB/loadLibrary)
    (let [opts (doto (Options.)
                 (.setCreateIfMissing true)
                 (.setMergeOperatorName "uint64add"))
          db (RocksDB/open opts (db-path db-name))]
      (assoc this :db db :options opts)))

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

  (close [{:keys [^RocksDB db ^Options options]}]
    (.close db)
    (.close options))

  (destroy [this]
    (with-open [options (Options.)]
      (RocksDB/destroyDB (db-path db-name) options))))

(defn crux-rocks-kv [db-name]
  (map->CruxRocksKv {:db-name db-name}))
