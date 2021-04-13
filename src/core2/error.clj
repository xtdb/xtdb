(ns core2.error)

(defn illegal-arg
  ([k] (illegal-arg k {}))

  ([k data]
   (illegal-arg k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Illegal argument: '%s'" k))]
     (core2.IllegalArgumentException. message
                                      (merge {::error-type :illegal-argument
                                              ::error-key k
                                              ::message message}
                                             data)
                                      cause))))
