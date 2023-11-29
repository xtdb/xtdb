(ns xtdb.james-bond
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]))

(def tx-ops
  (vec
   (for [doc (read-string (slurp (io/resource "james-bond.edn")))]
     (xt/put (keyword (:type doc))
             (-> doc
                 (dissoc :type)
                 ;; no sets as yet
                 (update :film/vehicles vec)
                 (update :film/bond-girls vec))))))
