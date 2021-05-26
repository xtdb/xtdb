(ns core2.james-bond
  (:require [clojure.java.io :as io]))

(def tx-ops
  (vec
   (for [doc (read-string (slurp (io/resource "james-bond.edn")))]
     {:op :put,
      ;; no cardinality many as yet (if at all?)
      :doc (->> (dissoc doc :film/vehicles :film/bond-girls)
                ;; nor keywords
                (into {} (map (juxt key
                                    (comp (fn [v]
                                            (if (keyword? v)
                                              (name v)
                                              v))
                                          val)))))})))
