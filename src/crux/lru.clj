(ns crux.lru
  (:require [crux.db :as db]
            [crux.index :as idx]
            [crux.kv-store :as ks])
  (:import java.io.Closeable
           [java.util Collections LinkedHashMap]
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

(defprotocol CacheProvider
  (get-named-cache [this cache-name cache-size]))

;; TODO: this should be changed to something more sensible, this is to
;; simplify API usage, and the kv instance is the main
;; object. Potentially these caches should simply just live in the
;; main system directly, but that requires passing more stuff around
;; to the lower levels.
(defrecord CacheProvidingKvStore [kv cache-state]
  ks/KvStore
  (open [this]
    (assoc this :kv (ks/open kv)))

  (new-snapshot [_]
    (ks/new-snapshot kv))

  (store [_ kvs]
    (ks/store kv kvs))

  (delete [_ ks]
    (ks/delete kv ks))

  (backup [_ dir]
    (ks/backup kv dir))

  (count-keys [_]
    (ks/count-keys kv))

  (db-dir [_]
    (ks/db-dir kv))

  (kv-name [_]
    (ks/kv-name kv))

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
  (->CacheProvidingKvStore kv (atom {})))

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

(def ^:const default-doc-cache-size 10240)

(defn new-cached-object-store
  ([kv]
   (new-cached-object-store kv default-doc-cache-size))
  ([kv cache-size]
   (->CachedObjectStore (get-named-cache kv ::doc-cache cache-size)
                        (idx/->KvObjectStore kv))))


(defn- ensure-iterator-open [closed-state]
  (when @closed-state
    (throw (IllegalStateException. "Iterator closed."))))

(defrecord CachedIterator [i closed-state]
  ks/KvIterator
  (seek [_ k]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/seek i k)))

  (next [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/next i)))

  (value [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/value i)))

  Closeable
  (close [_]
    (ensure-iterator-open closed-state)
    (reset! closed-state true)))

(defrecord CachedSnapshot [^Closeable snapshot close-snapshot? iterators-state]
  ks/KvSnapshot
  (new-iterator [_]
    (if-let [i (->> @iterators-state
                    (filter (comp deref :closed-state))
                    (first))]
      (if (compare-and-set! (:closed-state i) true false)
        i
        (recur))
      (let [i (->CachedIterator (ks/new-iterator snapshot) (atom false))]
        (swap! iterators-state conj i)
        i)))

  Closeable
  (close [_]
    (doseq [{:keys [^Closeable i closed-state]} @iterators-state]
      (locking i
        (reset! closed-state true)
        (.close i)))
    (when close-snapshot?
      (.close snapshot))))

(defn ^crux.lru.CachedSnapshot new-cached-snapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (atom #{})))
