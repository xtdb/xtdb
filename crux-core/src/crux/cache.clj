(ns ^:no-doc crux.cache
  (:require [crux.cache.second-chance]
            [crux.system :as sys])
  (:import crux.cache.ICache))

(defn compute-if-absent [^ICache cache k stored-key-fn f]
  (.computeIfAbsent cache k stored-key-fn f))

(defn evict [^ICache cache k]
  (.evict cache k))

(defn ->cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}}}
  [opts]
  (crux.cache.second-chance/->second-chance-cache opts))
