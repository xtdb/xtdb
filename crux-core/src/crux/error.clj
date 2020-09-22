(ns crux.error)

(defn illegal-arg
  ([k] (illegal-arg k {}))

  ([k {::keys [^String message] :as data}]
   (let [message (or message (format "Illegal argument: '%s'" k))]
     (crux.IllegalArgumentException. message
                                     (merge {::error-type :illegal-argument
                                             ::error-key k
                                             ::message message}
                                            data)))))
