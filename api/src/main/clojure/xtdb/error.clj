(ns xtdb.error
  (:import java.io.Writer))

(defn illegal-arg
  ([k] (illegal-arg k {}))

  ([k data]
   (illegal-arg k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Illegal argument: '%s'" (symbol k)))]
     (xtdb.IllegalArgumentException. k message (-> data (dissoc ::message)) cause))))

(defn runtime-err
  ([k] (runtime-err k {}))

  ([k data]
   (runtime-err k data nil))

  ([k {::keys [^String message] :as data} cause]
   (let [message (or message (format "Runtime error: '%s'" (symbol k)))]
     (xtdb.RuntimeException. k message (-> data (dissoc ::message)) cause))))
