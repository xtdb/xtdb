(ns ^:no-doc xtdb.cache.nop
  (:require [xtdb.system :as sys])
  (:import xtdb.cache.ICache
           java.util.Collections))

(deftype NopCache []
  Object
  (toString [_]
    (str (Collections/emptyMap)))

  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (f k))

  (evict [_ k])

  (valAt [_ k])

  (valAt [_ k default]
    default)

  (count [_] 0)

  (close [_]))

(defn ->nop-cache
  {::sys/args {:cache-size {:doc "Cache size"
                            :default 0
                            :spec ::sys/nat-int}}}
  ^xtdb.cache.ICache [{:keys [cache-size]}]
  (->NopCache))
