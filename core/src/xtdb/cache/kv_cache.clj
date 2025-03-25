(ns xtdb.cache.kv-cache
  (:require [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.kv :as kv]
            [xtdb.codec :as c]
            [xtdb.memory :as mem]))

(deftype KvCache [kv-store]
  xtdb.cache.ICache
  (computeIfAbsent [this k stored-key-fn f]
    (let [stored-k (c/->id-buffer (stored-key-fn k))]
      (or (some-> (kv/get-value (kv/new-snapshot kv-store) stored-k) mem/<-nippy-buffer)
          (let [v (f stored-k)]
            (kv/store kv-store [[stored-k (mem/->nippy-buffer v)]])
            v))))

  (evict [_ k]
    (let [stored-k (c/->id-buffer k)]
      (kv/store kv-store [[stored-k nil]])))

  (valAt [_ k]
    (some-> (kv/get-value (kv/new-snapshot kv-store) (c/->id-buffer k)) mem/<-nippy-buffer))

  (valAt [_ k default]
    (or (some-> (kv/get-value (kv/new-snapshot kv-store) (c/->id-buffer k)) mem/<-nippy-buffer)
        default))

  (count [_]
    (kv/count-keys kv-store))

  (close [_]
    (kv/compact kv-store)))

(defn ->kv-cache
  {::sys/deps {:kv-store 'xtdb.mem-kv/->kv-store}}
  [{:keys [kv-store]}]
  (->KvCache kv-store))
