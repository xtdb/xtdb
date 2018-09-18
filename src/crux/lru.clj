(ns crux.lru
  (:import [java.util Collections LinkedHashMap]
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
