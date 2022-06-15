(ns xtdb.kv.mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.memory :as mem])
  (:import clojure.lang.ISeq
           java.io.Closeable
           [java.util NavigableMap TreeMap]))

(deftype MutableKvIterator [^NavigableMap db, !tail-seq]
  kv/KvIterator
  (seek [_ k]
    (some-> (reset! !tail-seq (->> (.tailMap db (mem/as-buffer k) true)
                                   (filter val)))
            first
            key))

  (next [_]
    (some-> (swap! !tail-seq rest) first key))

  (prev [_]
    (loop []
      (when-let [[[k _] :as tail-seq] (seq @!tail-seq)]
        (when-let [[k v :as lower-entry] (.lowerEntry db k)]
          (reset! !tail-seq (cons lower-entry tail-seq))
          (if v
            k
            (recur))))))

  (value [_]
    (some-> (first @!tail-seq) val))

  Closeable
  (close [_]))

(deftype MutableKvSnapshot [^NavigableMap db]
  kv/KvSnapshot
  (new-iterator [this] (->MutableKvIterator db (atom nil)))
  (get-value [this k] (get db (mem/as-buffer k)))

  ISeq
  (seq [_] (seq db))

  Closeable
  (close [_]))

(deftype MutableKvTx [^TreeMap db, ^NavigableMap db2]
  kv/KvStoreTx

  (new-tx-snapshot [_]
    (->MutableKvSnapshot db2))

  (put-kv [_ k v]
    (.put db2
          (mem/copy-to-unpooled-buffer (mem/as-buffer k))
          (some-> v mem/as-buffer mem/copy-to-unpooled-buffer)))

  (commit-kv-tx [_]
    (.putAll db db2))

  Closeable
  (close [_]))

(deftype MutableKvStore [^TreeMap db]
  kv/KvStoreWithReadTransaction
  (begin-kv-tx [_]
    (->MutableKvTx db (.clone db)))

  kv/KvStore
  (new-snapshot ^java.io.Closeable [this]
    (->MutableKvSnapshot db))

  (store [_ kvs]
    (doseq [[k v] kvs]
      (.put db (mem/as-buffer k) (some-> v mem/as-buffer))))

  (fsync [_])
  (compact [_])
  (count-keys [_] (count db))
  (db-dir [_])
  (kv-name [this] (str (class this))))

(defn ->mutable-kv-store
  ([] (->mutable-kv-store nil))
  ([_] (->MutableKvStore (TreeMap. mem/buffer-comparator))))
