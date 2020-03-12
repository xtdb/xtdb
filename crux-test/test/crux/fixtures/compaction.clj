(ns crux.fixtures.compaction
  (:require [crux.compaction :as cc]
            [crux.fixtures.api :as fapi]
            [crux.node :as n]))

(defn with-compaction [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cc/module)
                      (assoc :crux.compaction/tt-vt-interval-s (* 60 60 24)))
    f))
