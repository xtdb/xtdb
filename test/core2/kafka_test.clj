(ns core2.kafka-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.ingester :as ingest]
            [core2.kafka :as k]
            [core2.local-node :as node]
            [core2.test-util :as tu]
            [core2.operator :as op])
  (:import java.util.UUID))

(t/deftest ^:requires-docker ^:kafka test-kafka
  (let [topic-name (str "core2.kafka-test." (UUID/randomUUID))]
    (with-open [node (node/start-node {::k/log {:topic-name topic-name}})]
      (let [tx (c2/submit-tx node [[:put {:id :foo}]])
            db (ingest/snapshot (tu/component node :core2/ingester) tx)]
        (t/is (= [{:id :foo}]
                 (op/query-ra '[:scan [id]] db)))))))
