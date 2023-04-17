(ns xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.kafka :as k]
            [xtdb.node :as node]
            [xtdb.test-util :as tu])
  (:import java.util.UUID))

(t/deftest ^:requires-docker ^:kafka test-kafka
  (let [topic-name (str "xtdb.kafka-test." (UUID/randomUUID))]
    (with-open [node (node/start-node {::k/log {:topic-name topic-name}})]
      (xt/submit-tx node [[:put {:xt/id :foo}]])

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table :xt_docs} [xt/id]]
                            {:node node}))))))
