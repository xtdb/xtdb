(ns core2.error
  (:import java.io.Writer))

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

(defmethod print-method core2.IllegalArgumentException
  [e, ^Writer w]
  (.write w (str "#core2/illegal-arg " (ex-data e))))

(defn -iae-reader [data]
  (illegal-arg (::error-key data) data))
