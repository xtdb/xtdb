(ns xtdb.error
  (:import java.io.Writer))

(defn illegal-arg
  ([k] (illegal-arg k {}))

  ([k data]
   (illegal-arg k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Illegal argument: '%s'" k))]
     (xtdb.IllegalArgumentException. message
                                      (merge {::error-type :illegal-argument
                                              ::error-key k
                                              ::message message}
                                             data)
                                      cause))))

(defmethod print-dup xtdb.IllegalArgumentException [e, ^Writer w]
  (.write w (str "#xt/illegal-arg " (ex-data e))))

(defmethod print-method xtdb.IllegalArgumentException [e, ^Writer w]
  (print-dup e w))

(defn -iae-reader [data]
  (illegal-arg (::error-key data) data))

(defn runtime-err
  ([k] (runtime-err k {}))

  ([k data]
   (runtime-err k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Runtime error: '%s'" k))]
     (xtdb.RuntimeException. message
                              (merge {::error-type :runtime-error
                                      ::error-key k
                                      ::message message}
                                     data)
                              cause))))

(defmethod print-dup xtdb.RuntimeException [e, ^Writer w]
  (.write w (str "#xt/runtime-err " (ex-data e))))

(defmethod print-method xtdb.RuntimeException [e, ^Writer w]
  (print-dup e w))

(defn -runtime-err-reader [data]
  (runtime-err (::error-key data) data))
