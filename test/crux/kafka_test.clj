(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]
            [crux.kafka :as k]
            [crux.fixtures :as f]
            [crux.embedded-kafka :as ek])
  (:import [org.apache.kafka.clients.consumer
            ConsumerRecord]
           [org.apache.kafka.clients.producer
            ProducerRecord]
           [org.apache.kafka.common TopicPartition]))

(t/use-fixtures :once ek/with-embedded-kafka-cluster)

(t/deftest test-can-produce-and-consume-message
  (let [topic "test-can-produce-and-consume-message-topic"
        person (f/random-person)
        partitions [(TopicPartition. topic 0)]]

    (with-open [p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*})]
      @(.send p (ProducerRecord. topic (.getBytes (pr-str person) "UTF-8"))))

    (with-open [c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-produce-and-consume-message-topic"})]
      (.assign c partitions)
      (.seekToBeginning c partitions)
      (let [records (.poll c 1000)]
        (t/is (= 1 (.count records)))
        (t/is (= person (some-> records
                                ^ConsumerRecord first
                                .value
                                byte-array
                                (String. "UTF-8")
                                edn/read-string)))))))

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps))))

(t/deftest test-can-transact-entities
  (let [topic "test-can-transact-entities"
        entities (load-ntriples-example  "crux/example-data-artists.nt")
        partitions [(TopicPartition. topic 0)]]

    (with-open [p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "transactional.id" "test-can-transact-entities"})]
      (.initTransactions p)
      (k/transact p topic entities))

    (with-open [c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-transact-entities"})]
      (.assign c partitions)
      (.seekToBeginning c partitions)
      (let [entities (map k/consumer-record->entity (.poll c 1000))]
        (t/is (= 7 (count entities)))
        (t/is (= {:http://xmlns.com/foaf/0.1/firstName "Pablo"
                  :http://xmlns.com/foaf/0.1/surname "Picasso"}
                 (select-keys (first entities)
                              [:http://xmlns.com/foaf/0.1/firstName
                               :http://xmlns.com/foaf/0.1/surname])))))))
