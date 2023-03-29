(ns xtdb.james-bond
  (:require [clojure.java.io :as io]))

(def tx-ops
  (vec
   (for [doc (read-string (slurp (io/resource "james-bond.edn")))]
     [:put (keyword (:type doc))
      (-> doc
          (dissoc :type)
          ;; no sets as yet
          (update :film/vehicles vec)
          (update :film/bond-girls vec))])))
