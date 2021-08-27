(ns xtdb.error)

(defn illegal-arg
  ([k] (illegal-arg k {}))

  ([k {::keys [^String message] :as data}]
   (illegal-arg k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Illegal argument: '%s'" k))]
     (xtdb.IllegalArgumentException. message
                                     (merge {::error-type :illegal-argument
                                             ::error-key k
                                             ::message message}
                                            data)
                                     cause))))

(defn node-out-of-sync [{:keys [requested available]}]
  (let [message (format "Node out of sync - requested '%s', available '%s'" requested available)]
    (xtdb.api.NodeOutOfSyncException. message
                                      {::error-type :node-out-of-sync
                                       ::message message
                                       :requested requested
                                       :available available})))
