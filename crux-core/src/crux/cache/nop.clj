(ns ^:no-doc crux.cache.nop
  (:require [crux.system :as sys])
  (:import crux.cache.ICache
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
  ^crux.cache.ICache [{:keys [cache-size]}]
  (->NopCache))
