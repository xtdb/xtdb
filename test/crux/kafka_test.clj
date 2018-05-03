(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.edn :as edn]
            [crux.fixtures :as f]
            [crux.embedded-kafka :as ek])
  (:import [java.util List Map]
           [org.apache.kafka.clients.consumer
            ConsumerRecord ConsumerRecords KafkaConsumer]
           [org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization
            StringDeserializer
            StringSerializer]))

(t/use-fixtures :each ek/with-embedded-kafka-cluster)

(t/deftest test-can-produce-and-consume-message
  (let [topic "test-topic"
        person (f/random-person)]

    (let [producer-config {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                           "key.serializer" (.getName StringSerializer)
                           "value.serializer" (.getName StringSerializer)}]
      (with-open [p (KafkaProducer. ^Map producer-config)]
        @(.send p (ProducerRecord. topic (pr-str person)))))

    (let [consumer-config {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                           "group.id" "test"
                           "enable.auto.commit" "false"
                           "key.deserializer" (.getName StringDeserializer)
                           "value.deserializer" (.getName StringDeserializer)}
          partitions [(TopicPartition. topic 0)]]
      (with-open [c (KafkaConsumer. ^Map consumer-config)]
        (.assign c partitions)
        (.seekToBeginning c partitions)
        (let [records (.poll c 1000)]
          (t/is (= 1 (.count records)))
          (t/is (= person (some-> records
                                  ^ConsumerRecord first
                                  .value
                                  edn/read-string))))))))
