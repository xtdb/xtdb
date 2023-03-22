(ns core2.james-bond
  (:require [clojure.java.io :as io]))

(def tx-ops
  (vec
   (for [doc (read-string (slurp (io/resource "james-bond.edn")))]
     [:put 'xt_docs
      (-> doc
          ;; no sets as yet
          (update :film/vehicles vec)
          (update :film/bond-girls vec))])))
