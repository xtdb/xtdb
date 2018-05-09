(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.core :as crux]
            [crux.kv :as kv]
            [crux.rdf :as rdf]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.fixtures :as f]
            [crux.test-utils :as tu]
            [crux.embedded-kafka :as ek])
  (:import [java.util List]
           [clojure.lang Keyword]
           [org.apache.kafka.clients.consumer
            ConsumerRecord]
           [org.apache.kafka.clients.producer
            ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization
            Serdes StringSerializer StringDeserializer LongDeserializer]
           [org.apache.kafka.streams
            KafkaStreams StreamsBuilder StreamsConfig]
           [org.apache.kafka.streams.kstream
            KStream Produced]))

(t/use-fixtures :once ek/with-embedded-kafka-cluster f/start-system)

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person (f/random-person)
        partitions [(TopicPartition. topic 0)]]

    (with-open [ac (k/create-admin-client {"bootstrap.servers" ek/*kafka-bootstrap-servers*})
                p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*})
                c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-produce-and-consume-message-using-embedded-kafka-topic"})]
      (k/create-topic ac topic 1 1 {})

      @(.send p (ProducerRecord. topic person))

      (.assign c partitions)
      (let [records (.poll c 1000)]
        (t/is (= 1 (count (seq records))))
        (t/is (= person (first (map k/consumer-record->value records))))))))

;; Based on:
;; https://github.com/confluentinc/kafka-streams-examples/blob/4.1.0-post/src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java
(t/deftest test-can-execute-kafka-streams-wordcount-example
  (let [input-topic "test-can-execute-kafka-streams-wordcount-example-input"
        output-topic "test-can-execute-kafka-streams-wordcount-example-output"
        partitions [(TopicPartition. output-topic 0)]]

    (with-open [ac (k/create-admin-client {"bootstrap.servers" ek/*kafka-bootstrap-servers*})
                p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "key.serializer" (.getName StringSerializer)
                                      "value.serializer" (.getName StringSerializer)})
                c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-execute-kafka-streams-wordcount-example-consumer"
                                      "key.deserializer" (.getName StringDeserializer)
                                      "value.deserializer" (.getName LongDeserializer)})]
      (doseq [topic [input-topic output-topic]]
        (k/create-topic ac topic 1 1 {}))

      (let [state-dir (tu/create-tmpdir "kafka-streams-state")
            config (StreamsConfig.
                    {"application.id" "test-can-execute-kafka-streams-wordcount-example-streams"
                     "bootstrap.servers" ek/*kafka-bootstrap-servers*
                     "default.key.serde" (.getName (.getClass (Serdes/String)))
                     "default.value.serde" (.getName (.getClass (Serdes/String)))
                     "commit.interval.ms" "1000"
                     "auto.offset.reset" "earliest"
                     "state.dir" (.getAbsolutePath state-dir)})
            builder (StreamsBuilder.)]
        (try
          (-> (.stream builder input-topic)
              (.flatMapValues (k/value-mapper
                               #(str/split (str/lower-case %) #"\W+")))
              (.groupBy (k/key-value-mapper
                         (fn [key word]
                           word)))
              .count
              .toStream
              (.to output-topic (Produced/with (Serdes/String)
                                               (Serdes/Long))))
          (with-open [streams (doto (KafkaStreams. (.build builder) config)
                                (.start))]

            (doseq [in ["Hello Kafka Streams"
                        "All streams lead to Kafka"
                        "Join Kafka Summit"]]
              @(.send p (ProducerRecord. input-topic in)))

            (.assign c partitions)
            (t/is (= {"hello" 1
                      "all" 1
                      "streams" 2
                      "lead" 1
                      "to" 1
                      "join" 1
                      "kafka" 3
                      "summit" 1}
                     (->> (for [^ConsumerRecord record (.poll c 5000)]
                            [(.key record)
                             (.value record)])
                          (into {})))))
          (finally
            (tu/delete-dir state-dir)))))))

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps))))

(t/deftest test-can-transact-entities
  (let [topic "test-can-transact-entities"
        entities (load-ntriples-example  "crux/example-data-artists.nt")
        partitions [(TopicPartition. topic 0)]]

    (with-open [ac (k/create-admin-client {"bootstrap.servers" ek/*kafka-bootstrap-servers*})
                p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "transactional.id" "test-can-transact-entities"})
                c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-transact-entities"})]
      (k/create-topic ac topic 1 1 {})

      (.initTransactions p)
      (k/transact p topic entities)

      (.assign c partitions)
      (let [entities (map k/consumer-record->value (.poll c 1000))]
        (t/is (= 7 (count entities)))
        (t/is (= {:http://xmlns.com/foaf/0.1/firstName "Pablo"
                  :http://xmlns.com/foaf/0.1/surname "Picasso"}
                 (select-keys (first entities)
                              [:http://xmlns.com/foaf/0.1/firstName
                               :http://xmlns.com/foaf/0.1/surname])))))))

;; TODO: schema less or generate on demand?
(defn transact-schema-based-on-entities [db entities]
  (doseq [s (reduce-kv
             (fn [s k v]
               (conj s {:crux.kv.attr/ident k
                        :crux.kv.attr/type (get {Keyword :keyword
                                                 String :string}
                                                (type v))}))
             #{} (apply merge entities))]
    (kv/transact-schema! db s)))

(t/deftest test-can-transact-and-query-entities
  (let [topic "test-can-transact-and-query-entities"
        entities (load-ntriples-example  "crux/picasso.nt")
        partitions [(TopicPartition. topic 0)]]

    (with-open [ac (k/create-admin-client {"bootstrap.servers" ek/*kafka-bootstrap-servers*})
                p (k/create-producer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "transactional.id" "test-can-transact-and-query-entities"})
                c (k/create-consumer {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                                      "group.id" "test-can-transact-and-query-entities"})]
      (k/create-topic ac topic 1 1 {})

      (transact-schema-based-on-entities f/*kv* entities)

      (.initTransactions p)
      (k/transact p topic entities)

      (.assign c partitions)
      (t/is (= 3 (count (k/consume-and-index-entities f/*kv* c))))

      ;; This is the client, or same person who transacted.
      (t/is (= (set (map (comp vector :crux.rdf/iri) entities))
               (q/q (crux/db f/*kv*)
                    '{:find [iri]
                      :where [[e :crux.rdf/iri iri]]})))

      (t/is (= #{[:http://example.org/Picasso]}
               (q/q (crux/db f/*kv*)
                    '{:find [iri]
                      :where [[e :http://xmlns.com/foaf/0.1/firstName "Pablo"]
                              [e :crux.rdf/iri iri]]}))))))
