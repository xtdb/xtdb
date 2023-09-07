(ns ^:no-doc xtdb.cache.lru
  (:require [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import xtdb.cache.ICache
           java.util.concurrent.locks.StampedLock
           java.util.function.Function
           [java.util LinkedHashMap Map]))

(set! *unchecked-math* :warn-on-boxed)

(deftype LRUCache [^LinkedHashMap cache ^StampedLock lock ^long size]
  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (let [v (.valAt this k ::not-found)] ; use ::not-found as values can be falsy
      (if (= ::not-found v)
        (let [k (stored-key-fn k)
              v (f k)]
          (xio/with-write-lock lock
            ;; lock the cache only after potentially heavy value and key calculations are done
            (.computeIfAbsent cache k (reify Function
                                        (apply [_ k]
                                          v)))))
        v)))

  (evict [_ k]
    (xio/with-write-lock lock
      (.remove cache k)))

  (valAt [_ k]
    (xio/with-write-lock lock
      (.get cache k)))

  (valAt [_ k default]
    (xio/with-write-lock lock
      (.getOrDefault cache k default)))

  (count [_]
    (.size cache))

  (close [_]
    (xio/with-write-lock lock
      (.clear cache))))

(defn ->lru-cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}}}
  ^xtdb.cache.ICache [{:keys [^long cache-size]
                       :or {cache-size (* 128 1024)}}]
  (let [cache (proxy [LinkedHashMap] [cache-size 0.75 true]
                (removeEldestEntry [_]
                  (> (.size ^Map this) cache-size)))
        lock (StampedLock.)]
    (->LRUCache cache lock cache-size)))
