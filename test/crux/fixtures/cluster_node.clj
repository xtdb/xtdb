(ns crux.fixtures.cluster-node
  (:require [crux.fixtures.kafka]
            [crux.fixtures.api :refer [*api*]]))

(defn with-cluster-node [f]
  (crux.fixtures.kafka/with-cluster-node
    (fn []
      (binding [*api* crux.fixtures.kafka/*cluster-node*]
        (f)))))
