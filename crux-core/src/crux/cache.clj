(ns ^:no-doc crux.cache
  (:require [crux.cache.lru :as lru])
  (:import crux.cache.ICache))

(defn compute-if-absent [^ICache cache k stored-key-fn f]
  (.computeIfAbsent cache k stored-key-fn f))

(defn evict [^ICache cache k]
  (.evict cache k))

(def new-cache lru/new-cache)
