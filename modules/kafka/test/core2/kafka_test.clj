(ns core2.kafka-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.kafka :as k]
            [core2.test-util :as tu])
  (:import java.time.Duration
           java.util.UUID
           org.apache.arrow.vector.util.Text))

(t/deftest ^:kafka test-kafka
  (let [topic-name (str "core2.kafka-test." (UUID/randomUUID))]
    (with-open [node (c2/start-node {:core2/log {:core2/module `k/->log
                                                 :topic-name topic-name
                                                 :create-topic? true}})]
      (let [tx @(c2/submit-tx node [{:op :put, :doc {:_id "foo"}}])]
        (c2/await-tx node tx (Duration/ofSeconds 1))
        (with-open [wm (c2/open-watermark node)
                    res (c2/open-q node wm '[:scan [_id]])]
          (t/is (= [{:_id (Text. "foo")}]
                   (into [] (mapcat seq) (tu/<-cursor res)))))))))
