(ns core2.kafka-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.kafka :as k])
  (:import java.util.UUID))

(t/deftest ^:kafka test-kafka
  (let [topic-name (str "core2.kafka-test." (UUID/randomUUID))]
    (with-open [node (c2/start-node {::k/log {:topic-name topic-name}})]
      (let [tx (c2/submit-tx node [{:op :put, :doc {:_id "foo"}}])]
        (c2/with-db [db node {:tx tx}]
          (t/is (= [{:_id "foo"}]
                   (into [] (c2/plan-ra '[:scan [_id]] db)))))))))
