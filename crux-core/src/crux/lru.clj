(ns crux.lru
  (:require [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv])
  (:import [clojure.lang Counted ILookup]
           java.io.Closeable
           java.util.concurrent.locks.StampedLock
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
                  v (f k)
                  stamp (.writeLock lock)]
              ; lock the cache only after potentially heavy value and key calculations are done
              (try
                (.computeIfAbsent cache k (reify Function
                                            (apply [_ k]
                                              v)))
                (finally
                  (.unlock lock stamp))))
            v)))

      (evict [_ k]
        (let [stamp (.writeLock lock)]
          (try
            (.remove cache k)
            (finally
              (.unlock lock stamp)))))

      ILookup
      (valAt [this k]
        (let [stamp (.writeLock lock)]
          (try
            (.get cache k)
            (finally
              (.unlock lock stamp)))))

      (valAt [this k default]
        (let [stamp (.writeLock lock)]
          (try
            (.getOrDefault cache k default)
            (finally
              (.unlock lock stamp)))))

      Counted
      (count [_]
        (.size cache)))))

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

  (prev [_]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (kv/prev i)
        (finally
          (.unlock lock stamp)))))

  (value [_]
    (let [stamp (.readLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (kv/value i)
        (finally
          (.unlock lock stamp)))))

  Closeable
  (close [_]
    (let [stamp (.writeLock lock)]
      (try
        (ensure-iterator-open closed-state)
        (reset! closed-state true)
        (finally
          (.unlock lock stamp))))))

(defrecord CachedSnapshot [^Closeable snapshot close-snapshot? ^StampedLock lock iterators-state]
  kv/KvSnapshot
  (new-iterator [_]
    (if-let [^CachedIterator i (->> @iterators-state
                                    (filter (fn [^CachedIterator i]
                                              @(.closed-state i)))
                                    (first))]
      (if (compare-and-set! (.closed-state i) true false)
        i
        (recur))
      (let [i (kv/new-iterator snapshot)
            i (->CachedIterator i lock (atom false))]
        (swap! iterators-state conj i)
        i)))

  (get-value [_ k]
    (kv/get-value snapshot k))

  Closeable
  (close [_]
    (doseq [^CachedIterator i @iterators-state]
      (let [stamp (.writeLock lock)]
        (try
          (reset! (.closed-state i) true)
          (.close ^Closeable (.i i))
          (finally
            (.unlock lock stamp)))))
    (when close-snapshot?
      (.close snapshot))))

(defn new-cached-snapshot ^crux.lru.CachedSnapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (StampedLock.) (atom #{})))

(defprotocol CacheProvider
  (get-named-cache [this cache-name]))

;; TODO: this should be changed to something more sensible, this is to
;; simplify API usage, and the kv instance is the main
;; object. Potentially these caches should simply just live in the
;; main node directly, but that requires passing more stuff around
;; to the lower levels.
(defrecord CacheProvidingKvStore [kv cache-state cache-size]
  kv/KvStore
  (open [this options]
    (assoc this :kv (kv/open kv options)))

  (new-snapshot [_]
    (new-cached-snapshot (kv/new-snapshot kv) true))

  (store [_ kvs]
    (kv/store kv kvs))

  (delete [_ ks]
    (kv/delete kv ks))

  (fsync [_]
    (kv/fsync kv))

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
  (get-named-cache [this cache-name]
    (get (swap! cache-state
                update
                cache-name
                (fn [cache]
                  (or cache (new-cache cache-size))))
         cache-name)))

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

(def ^:const default-query-cache-size 10240)

(def options
  (merge kv/options
         {::query-cache-size
          {:doc "Query Cache Size"
           :default default-query-cache-size
           :crux.config/type :crux.config/nat-int}}))

(defn start-kv-store ^java.io.Closeable [kv {:keys [crux.kv/check-and-store-index-version
                                                    crux.lru/query-cache-size] :as options}]
  (let [kv (if (instance? CacheProvidingKvStore kv)
             kv
             (->CacheProvidingKvStore kv (atom {}) query-cache-size))
        kv (kv/open kv options)]
    (try
      (if check-and-store-index-version
        (idx/check-and-store-index-version kv)
        kv)
      (catch Throwable t
        (.close ^Closeable kv)
        (throw t)))))
