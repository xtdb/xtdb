(ns ^:no-doc crux.cache.soft-values
  (:require [crux.system :as sys])
  (:import crux.cache.ICache
           crux.cache.soft_values.SoftReferenceWithKey
           [java.lang.ref Reference ReferenceQueue]
           java.util.function.Function
           java.util.Map
           java.util.concurrent.ConcurrentHashMap))

(declare cleanup-cache)

(defn- get-or-remove-reference [^Map cache ^SoftReferenceWithKey v-ref]
  (when v-ref
    (if-some [v (.get ^Reference v-ref)]
      v
      (do (.remove cache (.key v-ref) v-ref)
          nil))))

(deftype SoftValuesCache [^Map cache ^ReferenceQueue reference-queue]
  Object
  (toString [_]
    (str cache))

  ICache
  (computeIfAbsent [this k stored-key-fn f]
    (or (get-or-remove-reference cache (.get cache k))
        (let [k (stored-key-fn k)
              v (f k)
              v-ref (.computeIfAbsent cache k (reify Function
                                                (apply [_ k]
                                                  (SoftReferenceWithKey. k v reference-queue))))]
          (cleanup-cache this)
          v)))

  (evict [_ k]
    (.remove cache k))

  (valAt [_ k]
    (get-or-remove-reference cache (.get cache k)))

  (valAt [_ k default]
    (let [v-ref (.getOrDefault cache k default)]
      (if (= default v-ref)
        v-ref
        (get-or-remove-reference cache v-ref))))

  (count [_]
    (.size cache))

  (close [_]
    (.clear cache)))

(defn- cleanup-cache [^SoftValuesCache cache]
  (when-let [v-ref (.poll ^ReferenceQueue (.reference_queue cache))]
    (.remove ^Map (.cache cache) (.key ^SoftReferenceWithKey v-ref) v-ref)
    (recur cache)))

(defn ->soft-values-cache
  {::sys/args {:cache-size {:doc "Initial cache size"
                            :default (* 128 1024)
                            :spec ::sys/nat-int}}}
  ^crux.cache.ICache [{:keys [^long cache-size]
                       :or {cache-size (* 128 1024)}}]
  (->SoftValuesCache (ConcurrentHashMap. cache-size) (ReferenceQueue.)))
