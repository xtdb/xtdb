(ns crux.embedded-kafka-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.fixtures.kafka :as fk]
            [crux.kafka :as k])
  (:import java.time.Duration
           org.apache.kafka.clients.consumer.ConsumerRecord
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.TopicPartition))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fk/with-kafka-client)

(defn- consumer-record->value [^ConsumerRecord record]
  (.value record))

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person {:crux.db/id "foo"}
        partitions [(TopicPartition. topic 0)]]

    (k/create-topic fk/*admin-client* topic 1 1 {})

    @(.send fk/*producer* (ProducerRecord. topic person))

    (.assign fk/*consumer* partitions)
    (let [records (.poll fk/*consumer* (Duration/ofMillis 10000))]
      (t/is (= 1 (count (seq records))))
      (t/is (= person (first (map consumer-record->value records)))))))
