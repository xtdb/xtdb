(ns ^:no-doc crux.cache
  (:require [crux.cache.lru :as lru]
            [crux.system :as sys])
  (:import crux.cache.ICache))

(defn compute-if-absent [^ICache cache k stored-key-fn f]
  (.computeIfAbsent cache k stored-key-fn f))

(defn evict [^ICache cache k]
  (.evict cache k))

(def ->cache lru/->lru-cache)
