(ns crux.fixtures.standalone
  (:require [crux.fixtures.api :as fapi]
            [crux.io :as cio]
            [crux.standalone :as standalone]
            [crux.node :as n]))

(defn with-standalone-node [f]
  (fapi/with-opts {::n/topology '[crux.standalone/topology]}
    f))

(defn with-standalone-doc-store [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj (-> standalone/topology
                                                    (select-keys [::n/document-store
                                                                  ::standalone/event-log]))))
    f))
