(ns crux.rocksdb
  (:require [clojure.java.io :as io]
            [crux.kv-store :refer :all]
            [crux.byte-utils :as bu])
  (:import [java.io Closeable]
           [org.rocksdb Options ReadOptions RocksDB RocksIterator Slice]))

(defn- -value [^RocksDB db k]
  (with-open [i (.newIterator db)]
    (.seek i k)
    (when (and (.isValid i) (zero? (bu/compare-bytes (.key i) k Integer/MAX_VALUE)))
      (.value i))))

(defn- -seek [^RocksDB db k]
  (with-open [i (.newIterator db)]
    (.seek i k)
    (when (.isValid i)
      [(.key i) (.value i)])))

(defn- rock-iterator-loop [^RocksIterator i key-pred f init]
  (loop [init' init]
    (if (and (.isValid i) (key-pred (.key i)))
      (let [result (f init' [(.key i) (.value i)])]
        (if (reduced? result)
          @result
          (do
            (.next i)
            (recur result))))
      init')))

;; TODO move to IReduceInit
(defn- -seek-and-iterate
  [^RocksDB db ^ReadOptions read-options key-pred k]
  (reify clojure.lang.IReduce
    (reduce [this f]
      (with-open [read-options read-options
                  i ^RocksIterator (.newIterator db read-options)]
        (.seek i k)
        (if (and (.isValid i) (key-pred (.key i)))
          (rock-iterator-loop i key-pred f [(.key i) (.value i)])
          (f))))
    (reduce [this f init]
      (with-open [read-options read-options
                  i ^RocksIterator (.newIterator db read-options)]
        (.seek i k)
        (rock-iterator-loop i key-pred f init)))))

(defrecord CruxRocksKv [db-dir]
  CruxKvStore
  (open [this]
    (RocksDB/loadLibrary)
    (let [opts (doto (Options.)
                 (.setCreateIfMissing true)
                 (.setMergeOperatorName "uint64add"))
          db (RocksDB/open opts (.getAbsolutePath (io/file db-dir)))]
      (assoc this :db db :options opts)))

  (value [{:keys [db]} k]
    (-value db k))

  (seek [{:keys [db]} k]
    (-seek db k))

  (seek-and-iterate [{:keys [^RocksDB db]} k upper-bound]
    (let [read-options (.setIterateUpperBound (ReadOptions.) (Slice. #^bytes upper-bound))]
      (-seek-and-iterate db read-options (constantly true) k)))

  (seek-and-iterate-bounded [{:keys [^RocksDB db]} k]
    (let [array-length (alength #^bytes k)
          pred-fn #(zero? (bu/compare-bytes k % array-length))]
      (-seek-and-iterate db (ReadOptions.) pred-fn k)))

  (store [{:keys [^RocksDB db]} k v]
    (.put db k v))

  (merge! [{:keys [^RocksDB db]} k v]
    (.merge db k v))

  (destroy [this]
    (with-open [options (Options.)]
      (RocksDB/destroyDB (.getAbsolutePath (io/file db-dir)) options)))

  Closeable
  (close [{:keys [^RocksDB db ^Options options]}]
    (.close db)
    (.close options)))
