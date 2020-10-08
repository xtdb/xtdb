(ns ^:no-doc crux.cache.lru
  (:require [crux.io :as cio]
            [crux.system :as sys])
  (:import crux.cache.ICache
           java.util.concurrent.locks.StampedLock
           java.util.function.Function
           [java.util Collections LinkedHashMap Map]))

(set! *unchecked-math* :warn-on-boxed)

(deftype LRUCache [^LinkedHashMap cache ^StampedLock lock ^long size]
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

  (keySet [_]
    (cio/with-write-lock lock
      (Collections/unmodifiableSet (.keySet cache))))

  (valAt [_ k]
    (cio/with-write-lock lock
      (.get cache k)))

  (valAt [_ k default]
    (cio/with-write-lock lock
      (.getOrDefault cache k default)))

  (count [_]
    (.size cache))

  (close [_]
    (cio/with-write-lock lock
      (.clear cache))))

(defn ->lru-cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}}}
  ^crux.cache.ICache [{:keys [^long cache-size]}]
  (let [cache (proxy [LinkedHashMap] [cache-size 0.75 true]
                (removeEldestEntry [_]
                  (> (.size ^Map this) cache-size)))
        lock (StampedLock.)]
    (->LRUCache cache lock cache-size)))
