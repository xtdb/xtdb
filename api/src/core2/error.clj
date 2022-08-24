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

(defmethod print-dup core2.IllegalArgumentException [e, ^Writer w]
  (.write w (str "#c2/illegal-arg " (ex-data e))))

(defmethod print-method core2.IllegalArgumentException [e, ^Writer w]
  (print-dup e w))

(defn -iae-reader [data]
  (illegal-arg (::error-key data) data))

(defn runtime-err
  ([k] (runtime-err k {}))

  ([k data]
   (runtime-err k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Illegal argument: '%s'" k))]
     (core2.RuntimeException. message
                              (merge {::error-type :illegal-argument
                                      ::error-key k
                                      ::message message}
                                     data)
                              cause))))

(defmethod print-dup core2.RuntimeException [e, ^Writer w]
  (.write w (str "#c2/runtime-err " (ex-data e))))

(defmethod print-method core2.RuntimeException [e, ^Writer w]
  (print-dup e w))

(defn -runtime-err-reader [data]
  (runtime-err (::error-key data) data))
