(ns crux.fixtures.standalone
  (:require [crux.fixtures.api :as apif]
            [crux.io :as cio]))

(defn with-standalone-node [f]
  (apif/with-opts {:crux.node/topology '[crux.standalone/topology]}
    f))
