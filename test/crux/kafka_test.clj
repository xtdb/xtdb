(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.core :as crux]
            [crux.rdf :as rdf]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.fixtures :as f]
            [crux.embedded-kafka :as ek])
  (:import [java.util List]
           [clojure.lang Keyword]
           [org.apache.kafka.clients.consumer
            ConsumerRecord]
           [org.apache.kafka.clients.producer
            ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization
            Serdes StringSerializer StringDeserializer LongDeserializer]))

(t/use-fixtures :once ek/with-embedded-kafka-cluster)
(t/use-fixtures :each ek/with-kafka-client f/with-kv-store)

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person (f/random-person)
        partitions [(TopicPartition. topic 0)]]

    (k/create-topic ek/*admin-client* topic 1 1 {})

    @(.send ek/*producer* (ProducerRecord. topic person))

    (.assign ek/*consumer* partitions)
    (let [records (.poll ek/*consumer* 10000)]
      (t/is (= 1 (count (seq records))))
      (t/is (= person (first (map k/consumer-record->value records)))))))

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (doall))))

(t/deftest test-can-transact-entities
  (let [topic "test-can-transact-entities"
        entities (load-ntriples-example  "crux/example-data-artists.nt")
        indexer (crux/indexer f/*kv*)]

    (k/create-topic ek/*admin-client* topic 1 1 {})
    (k/subscribe-from-stored-offsets indexer ek/*consumer* topic)

    (k/transact ek/*producer* topic entities)

    (let [txs (map k/consumer-record->value (.poll ek/*consumer* 5000))]
      (t/is (= 1 (count txs)))
      (t/is (= {:http://xmlns.com/foaf/0.1/firstName "Pablo"
                :http://xmlns.com/foaf/0.1/surname "Picasso"}
               (select-keys (ffirst txs)
                            [:http://xmlns.com/foaf/0.1/firstName
                             :http://xmlns.com/foaf/0.1/surname]))))))

(t/deftest test-can-transact-and-query-entities
  (let [topic "test-can-transact-and-query-entities"
        entities (load-ntriples-example  "crux/picasso.nt")
        indexer (crux/indexer f/*kv*)]

    (k/create-topic ek/*admin-client* topic 1 1 {})
    (k/subscribe-from-stored-offsets indexer ek/*consumer* topic)

    (t/testing "transacting and indexing"
      (k/transact ek/*producer* topic entities)

      (t/is (= 3 (count (k/consume-and-index-entities indexer ek/*consumer*))))
      (t/is (empty? (.poll ek/*consumer* 1000))))

    (t/testing "restoring to stored offsets"
      (.seekToBeginning ek/*consumer* (.assignment ek/*consumer*))
      (k/seek-to-stored-offsets indexer ek/*consumer* (.assignment ek/*consumer*))
      (t/is (empty? (.poll ek/*consumer* 1000))))

    (t/testing "querying transacted data"
      (t/is (= (set (map (comp vector :crux.rdf/iri) entities))
               (q/q (crux/db f/*kv*)
                    '{:find [iri]
                      :where [[e :crux.rdf/iri iri]]})))

      (t/is (= #{[:http://example.org/Picasso]}
               (q/q (crux/db f/*kv*)
                    '{:find [iri]
                      :where [[e :http://xmlns.com/foaf/0.1/firstName "Pablo"]
                              [e :crux.rdf/iri iri]]}))))))

(t/deftest test-can-transact-and-query-dbpedia-entities
  (let [topic "test-can-transact-and-query-dbpedia-entities"
        entities (-> (concat (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                             (load-ntriples-example "crux/Guernica_(Picasso).ntriples"))
                     (rdf/use-default-language :en))
        indexer (crux/indexer f/*kv*)]

    (k/create-topic ek/*admin-client* topic 1 1 {})
    (k/subscribe-from-stored-offsets indexer ek/*consumer* topic)

    (t/testing "transacting and indexing"
      (k/transact ek/*producer* topic entities)
      (t/is (= 2 (count (k/consume-and-index-entities indexer ek/*consumer*)))))

    (t/testing "querying transacted data"
      (t/is (= #{[:http://dbpedia.org/resource/Pablo_Picasso]}
               (q/q (crux/db f/*kv*)
                    '{:find [iri]
                      :where [[e :http://xmlns.com/foaf/0.1/givenName "Pablo"]
                              [e :crux.rdf/iri iri]]})))

      (t/is (= #{[(keyword "http://dbpedia.org/resource/Guernica_(Picasso)")]}
               (q/q (crux/db f/*kv*)
                    '{:find [g-iri]
                      :where [[p :http://xmlns.com/foaf/0.1/givenName "Pablo"]
                              [p :crux.rdf/iri p-iri]
                              [g :http://dbpedia.org/ontology/author p-iri]
                              [g :crux.rdf/iri g-iri]]}))))))
