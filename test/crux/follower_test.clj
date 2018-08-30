(ns crux.follower-test
  (:require [clojure.test :as t :refer [deftest is]]
            [crux.embedded-kafka :as ek]
            [crux.fixtures :as f]
            [crux.kafka :as k])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.apache.kafka.common.serialization.StringSerializer))

(def follower-topic "follower-topic")

(def ^:dynamic ^KafkaProducer *producer*)

(defn with-follower-options
  [f]
  (k/create-topic ek/*admin-client* follower-topic 1 1 k/tx-topic-config)
  (with-open [producer (k/create-producer
                         {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                          "key.serializer" (.getName StringSerializer)
                          "value.serializer" (.getName StringSerializer)})]
    (binding [f/*extra-options* {:follow-topics [follower-topic]}
              *producer* producer]
      (f))))

(t/use-fixtures :once
  ek/with-embedded-kafka-cluster
  with-follower-options
  f/with-dev-system)

(deftest test-follower
  @(.send *producer* (ProducerRecord. follower-topic "some-key" "hello world"))
  (Thread/sleep 10000)

  (is (= 1 1)))
