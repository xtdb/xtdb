(ns ^:no-doc crux.lru
  (:require [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv])
  (:import [clojure.lang Counted ILookup]
           java.io.Closeable
           java.util.concurrent.locks.StampedLock
           java.util.concurrent.atomic.AtomicBoolean
           java.util.function.Function
           java.util.LinkedHashMap))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol LRUCache
  (compute-if-absent [this k stored-key-fn f])
  ; key-fn sometimes used to copy the key to prevent memory leaks
  (evict [this k]))

(defn new-cache [^long size]
  (let [cache (proxy [LinkedHashMap] [size 0.75 true]
                (removeEldestEntry [_]
                  (> (count this) size)))
        lock (StampedLock.)]
    (reify
      Object
      (toString [this]
        (.toString cache))

      LRUCache
      (compute-if-absent [this k stored-key-fn f]
        (let [v (.valAt this k ::not-found)] ; use ::not-found as values can be falsy
          (if (= ::not-found v)
            (let [k (stored-key-fn k)
                  v (f k)]
              (cio/with-write-lock lock
                ;; lock the cache only after potentially heavy value and key calculations are done
                (.computeIfAbsent cache k (reify Function
                                            (apply [_ k]
                                              v)))))
            v)))

      (evict [_ k]
        (cio/with-write-lock lock
          (.remove cache k)))

      ILookup
      (valAt [this k]
        (cio/with-write-lock lock
          (.get cache k)))

      (valAt [this k default]
        (cio/with-write-lock lock
          (.getOrDefault cache k default)))

      Counted
      (count [_]
        (.size cache)))))

(defn- ensure-iterator-open [^AtomicBoolean closed-state]
  (when (.get closed-state)
    (throw (IllegalStateException. "Iterator closed."))))

(defrecord CachedIterator [i ^StampedLock lock ^AtomicBoolean closed-state]
  kv/KvIterator
  (seek [_ k]
    (cio/with-read-lock lock
      (ensure-iterator-open closed-state)
      (kv/seek i k)))

  (next [_]
    (cio/with-read-lock lock
      (ensure-iterator-open closed-state)
      (kv/next i)))

  (prev [_]
    (cio/with-read-lock lock
      (ensure-iterator-open closed-state)
      (kv/prev i)))

  (value [_]
    (cio/with-read-lock lock
      (ensure-iterator-open closed-state)
      (kv/value i)))

  Closeable
  (close [_]
    (cio/with-write-lock lock
      (ensure-iterator-open closed-state)
      (.set closed-state true))))

(defrecord CachedSnapshot [^Closeable snapshot close-snapshot? ^StampedLock lock iterators-state]
  kv/KvSnapshot
  (new-iterator [_]
    (if-let [^CachedIterator i (->> @iterators-state
                                    (filter (fn [^CachedIterator i]
                                              (.get ^AtomicBoolean (.closed-state i))))
                                    (first))]
      (if (.compareAndSet ^AtomicBoolean (.closed-state i) true false)
        i
        (recur))
      (let [i (kv/new-iterator snapshot)
            i (->CachedIterator i lock (AtomicBoolean.))]
        (swap! iterators-state conj i)
        i)))

  (get-value [_ k]
    (kv/get-value snapshot k))

  Closeable
  (close [_]
    (doseq [^CachedIterator i @iterators-state]
      (cio/with-write-lock lock
        (.set ^AtomicBoolean (.closed-state i) true)
        (.close ^Closeable (.i i))))

    (when close-snapshot?
      (.close snapshot))))

(defn new-cached-snapshot ^crux.lru.CachedSnapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (StampedLock.) (atom #{})))

(defrecord CachedSnapshotKvStore [kv]
  kv/KvStore
  (new-snapshot [_] (new-cached-snapshot (kv/new-snapshot kv) true))
  (store [_ kvs] (kv/store kv kvs))
  (delete [_ ks] (kv/delete kv ks))
  (fsync [_] (kv/fsync kv))
  (compact [_] (kv/compact kv))
  (backup [_ dir] (kv/backup kv dir))
  (count-keys [_] (kv/count-keys kv))
  (db-dir [_] (kv/db-dir kv))
  (kv-name [_] (kv/kv-name kv))

  Closeable
  (close [_] (.close ^Closeable kv)))

;; TODO: The options are kept to avoid having to change all kv stores
;; using wrap-lru-cache.
(def options {})

(defn wrap-lru-cache ^java.io.Closeable [kv _]
  (cond-> kv
    (not (instance? CachedSnapshotKvStore kv)) (->CachedSnapshotKvStore)))

(defrecord CachedIndex [idx index-cache]
  db/Index
  (db/seek-values [this k]
    (compute-if-absent index-cache k identity
                       (fn [k]
                           (db/seek-values idx k))))

  (db/next-values [this]
    (throw (UnsupportedOperationException.))))

(defn new-cached-index [idx cache-size]
  (->CachedIndex idx (new-cache cache-size)))
