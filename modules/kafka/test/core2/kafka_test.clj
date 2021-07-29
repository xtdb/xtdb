(ns core2.kafka-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.kafka :as k]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.operator :as op])
  (:import java.util.UUID))

(t/deftest ^:kafka test-kafka
  (let [topic-name (str "core2.kafka-test." (UUID/randomUUID))]
    (with-open [node (c2/start-node {::k/log {:topic-name topic-name}})]
      (let [tx (c2/submit-tx node [[:put {:_id "foo"}]])
            db (snap/snapshot (tu/component node ::snap/snapshot-factory) tx)]
        (t/is (= [{:_id "foo"}]
                 (into [] (op/plan-ra '[:scan [_id]] db))))))))
