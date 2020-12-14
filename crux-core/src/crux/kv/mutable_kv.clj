(ns crux.kv.mutable-kv
  (:require [crux.kv :as kv]
            [crux.memory :as mem])
  (:import clojure.lang.ISeq
           java.io.Closeable
           [java.util NavigableMap TreeMap]))

(deftype MutableKvIterator [^NavigableMap db, !tail-seq]
  kv/KvIterator
  (seek [this k]
    (some-> (reset! !tail-seq (seq (.tailMap db (mem/as-buffer k) true))) first key))

  (next [this]
    (some-> (swap! !tail-seq rest) first key))

  (prev [this]
    (when-let [[[k] :as tail-seq] (seq @!tail-seq)]
      (some-> (reset! !tail-seq (when-let [le (.lowerEntry db k)]
                                  (cons le tail-seq)))
              first
              key)))

  (value [this]
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

(deftype MutableKvStore [^NavigableMap db]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [this]
    (->MutableKvSnapshot db))

  (store [this kvs]
    (doseq [[k v] kvs]
      (.put db (mem/as-buffer k) (mem/as-buffer v))))

  (delete [this ks]
    (doseq [k ks]
      (.remove db (mem/as-buffer k))))

  (fsync [this])
  (compact [this])
  (count-keys [this] (count db))
  (db-dir [this])
  (kv-name [this] (str (class this))))

(defn ->mutable-kv-store
  ([] (->mutable-kv-store {}))
  ([_] (->MutableKvStore (TreeMap. mem/buffer-comparator))))
