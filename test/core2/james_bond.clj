(ns core2.james-bond
  (:require [clojure.java.io :as io]))

(def tx-ops
  (vec
   (for [doc (read-string (slurp (io/resource "james-bond.edn")))]
     [:put
      ;; no cardinality many as yet, see #574
      (->> (dissoc doc :film/vehicles :film/bond-girls)
           ;; nor namespaced keywords, see #573
           (into {} (map (letfn [(un-ns-kw [v]
                                   (if (qualified-keyword? v)
                                     (keyword (str (namespace v) "--" (name v)))
                                     v))]
                           (juxt (comp un-ns-kw key)
                                 (comp un-ns-kw val))))))])))
