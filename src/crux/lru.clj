(ns crux.lru
  (:require [clojure.spec.alpha :as s]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv])
  (:import java.io.Closeable
           [java.util Collections LinkedHashMap]
           java.util.concurrent.locks.StampedLock
           java.util.function.Function))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol LRUCache
  (compute-if-absent [this k f])
  (evict [this k]))

(defn new-cache [^long size]
  (Collections/synchronizedMap
    (proxy [LinkedHashMap] [size 0.75 true]
      (removeEldestEntry [_]
        (> (count this) size)))))

(extend-type java.util.Collections$SynchronizedMap
  LRUCache
  (compute-if-absent [this k f]
    (.computeIfAbsent this k (reify Function
                               (apply [_ k]
                                 (f k)))))
  (evict [this k]
    (.remove this k)))

(defrecord CachedObjectStore [cache object-store]
  db/ObjectStore
  (get-objects [this snapshot ks]
    (->> (for [k ks]
           [k (compute-if-absent
               cache
               k
               #(get (db/get-objects object-store snapshot [%]) %))])
         (into {})))

  (put-objects [this kvs]
    (db/put-objects object-store kvs))

  (delete-objects [this ks]
    (doseq [k ks]
      (evict cache k))
    (db/delete-objects object-store ks))

  Closeable
  (close [_]))

(defn- ensure-iterator-open [closed-state]
  (when @closed-state
    (throw (IllegalStateException. "Iterator closed."))))

(defrecord CachedIterator [i ^StampedLock lock closed-state]
  kv/KvIterator
  (seek [_ k]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (kv/seek i k)
        (finally
          (.unlock lock stamp)))))

  (next [_]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (kv/next i)
        (finally
          (.unlock lock stamp)))))

  (value [_]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (kv/value i)
        (finally
          (.unlock lock stamp)))))

  (refresh [this]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (assoc this :i (kv/refresh i))
        (finally
          (.unlock lock stamp)))))

  Closeable
  (close [_]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (reset! closed-state true)
        (finally
          (.unlock lock stamp))))))

(defrecord CachedSnapshot [^Closeable snapshot close-snapshot? ^StampedLock lock iterators-state]
  kv/KvSnapshot
  (new-iterator [_]
    (if-let [i (->> @iterators-state
                    (filter (comp deref :closed-state))
                    (first))]
      (if (compare-and-set! (:closed-state i) true false)
        (kv/refresh i)
        (recur))
      (let [i (->CachedIterator (kv/new-iterator snapshot) lock (atom false))]
        (swap! iterators-state conj i)
        i)))

  Closeable
  (close [_]
    (doseq [{:keys [^Closeable i closed-state]} @iterators-state]
      (let [stamp (.writeLock lock)]
        (try
          (reset! closed-state true)
          (.close i)
          (finally
            (.unlock lock stamp)))))
    (when close-snapshot?
      (.close snapshot))))

(defn new-cached-snapshot ^crux.lru.CachedSnapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (StampedLock.) (atom #{})))

(defprotocol CacheProvider
  (get-named-cache [this cache-name cache-size]))

;; TODO: this should be changed to something more sensible, this is to
;; simplify API usage, and the kv instance is the main
;; object. Potentially these caches should simply just live in the
;; main system directly, but that requires passing more stuff around
;; to the lower levels.
(defrecord CacheProvidingKvStore [kv cache-state]
  kv/KvStore
  (open [this options]
    (assoc this :kv (kv/open kv options)))

  (new-snapshot [_]
    (new-cached-snapshot (kv/new-snapshot kv) true))

  (store [_ kvs]
    (kv/store kv kvs))

  (delete [_ ks]
    (kv/delete kv ks))

  (backup [_ dir]
    (kv/backup kv dir))

  (count-keys [_]
    (kv/count-keys kv))

  (db-dir [_]
    (kv/db-dir kv))

  (kv-name [_]
    (kv/kv-name kv))

  Closeable
  (close [_]
    (.close ^Closeable kv))

  CacheProvider
  (get-named-cache [this cache-name cache-size]
    (get (swap! cache-state
                update
                cache-name
                (fn [cache]
                  (or cache (new-cache cache-size))))
         cache-name)))

(defn new-cache-providing-kv-store [kv]
  (if (instance? CacheProvidingKvStore kv)
    kv
    (->CacheProvidingKvStore kv (atom {}))))

(s/def ::doc-cache-size nat-int?)

(def ^:const default-doc-cache-size 10240)

(defn new-cached-object-store
  ([kv]
   (new-cached-object-store kv default-doc-cache-size))
  ([kv cache-size]
   (->CachedObjectStore (get-named-cache kv ::doc-cache (or cache-size default-doc-cache-size))
                        (idx/->KvObjectStore kv))))
