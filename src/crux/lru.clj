(ns crux.lru
  (:require [crux.kv-store :as ks])
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
