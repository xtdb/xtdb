(ns crux.fixtures.test-data
  (:require [clojure.java.io :as io]
            [crux.fixtures :as fix]))

(def james-bond-data
  (when-let [bond-file (io/resource "data/james-bond.edn")]
    (read-string (slurp bond-file))))

(defn with-james-bond-data [f]
  (fix/submit+await-tx (mapv (fn [e] [:xt/put e]) james-bond-data))
  (f))
