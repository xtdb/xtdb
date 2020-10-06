(ns ^:no-doc crux.cache.lru
  (:require [crux.io :as cio])
  (:import crux.cache.ICache
           java.util.concurrent.locks.StampedLock
           java.util.function.Function
           [java.util LinkedHashMap Map]))

(set! *unchecked-math* :warn-on-boxed)

(defn new-lru-cache [^long size]
  (let [cache (proxy [LinkedHashMap] [size 0.75 true]
                (removeEldestEntry [_]
                  (> (.size ^Map this) size)))
        lock (StampedLock.)]
    (reify
      Object
      (toString [_]
        (.toString cache))

      ICache
      (computeIfAbsent [this k stored-key-fn f]
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

      (valAt [_ k]
        (cio/with-write-lock lock
          (.get cache k)))

      (valAt [_ k default]
        (cio/with-write-lock lock
          (.getOrDefault cache k default)))

      (count [_]
        (.size cache))

      (close [_]))))
