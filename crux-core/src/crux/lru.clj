(ns ^:no-doc crux.lru
  (:require [crux.db :as db]
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
