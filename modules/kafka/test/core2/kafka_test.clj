(ns core2.kafka-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.kafka :as k]
            [core2.local-node :as node]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.operator :as op])
  (:import java.util.UUID))

(t/deftest ^:requires-docker test-kafka
  (let [topic-name (str "core2.kafka-test." (UUID/randomUUID))]
    (with-open [node (node/start-node {::k/log {:topic-name topic-name}})]
      (let [tx (c2/submit-tx node [[:put {:_id :foo}]])
            db (snap/snapshot (tu/component node ::snap/snapshot-factory) tx)]
        (t/is (= [{:_id :foo}]
                 (op/query-ra '[:scan [_id]] db)))))))
