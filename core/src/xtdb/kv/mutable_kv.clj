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

(deftype MutableKvTx [^NavigableMap db]
  kv/KvStoreTx

  (new-tx-snapshot [_]
    (->MutableKvSnapshot db))

  (put-kv [_ k v]
    (.put db
          (mem/copy-to-unpooled-buffer (mem/as-buffer k))
          (some-> v mem/as-buffer mem/copy-to-unpooled-buffer)))

  (commit-kv-tx [_])

  Closeable
  (close [_]))

(deftype MutableKvStore [^NavigableMap db]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [this]
    (->MutableKvSnapshot db))

  (begin-kv-tx [_]
    (->MutableKvTx db))

  (fsync [_])
  (compact [_])
  (count-keys [_] (count db))
  (db-dir [_])
  (kv-name [this] (str (class this))))

(defn ->mutable-kv-store
  ([] (->mutable-kv-store (TreeMap. mem/buffer-comparator)))
  ([^NavigableMap db] (->MutableKvStore db)))
