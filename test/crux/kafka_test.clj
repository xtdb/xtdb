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

;; Download from http://wiki.dbpedia.org/services-resources/ontology
;; mappingbased_properties_en.nt is the main data.
;; instance_types_en.nt contains type definitions only.
;; specific_mappingbased_properties_en.nt contains extra literals.
;; dbpedia_2014.owl is the OWL schema, not dealt with.

;; Test assumes these files are living under ../dbpedia related to the
;; crux project (to change).

;; There are 5053979 entities across 33449633 triplets in
;; mappingbased_properties_en.nt.

;; 1.6G    /tmp/kafka-log1572248494326726941
;; 2.0G    /tmp/kv-store8136342842355297151

;; 583800ms  ~9.7mins transact
;; 861799ms ~14.40mins index


;; Could use test selectors.
(def run-dbpedia-tests? false)

(t/deftest test-can-transact-all-dbpedia-entities
  (let [topic "test-can-transact-all-dbpedia-entities"
        indexer (crux/indexer f/*kv*)
        tx-size 1000
        max-limit Long/MAX_VALUE
        print-size 100000
        transacted (atom -1)
        mappingbased-properties-file (io/file "../dbpedia/mappingbased_properties_en.nt")]

    (if (and run-dbpedia-tests? (.exists mappingbased-properties-file))
      (do (k/create-topic ek/*admin-client* topic 1 1 {})
          (k/subscribe-from-stored-offsets indexer ek/*consumer* topic)

          (t/testing "transacting and indexing"
            (with-open [in (io/input-stream mappingbased-properties-file)]
              (future
                (time
                 (reset! transacted (reduce (fn [n entities]
                                              (k/transact ek/*producer* topic entities)
                                              (let [n (+ n (count entities))]
                                                (when (zero? (mod n print-size))
                                                  (println "transacted" n))
                                                n))
                                            0
                                            (->> (rdf/ntriples-seq in)
                                                 (rdf/statements->maps)
                                                 (map #(rdf/use-default-language % :en))
                                                 (take max-limit)
                                                 (partition-all tx-size))))))
              (time
               (loop [entities (k/consume-and-index-entities indexer ek/*consumer* 100)
                      n 0]
                 (when-not (= n @transacted)
                   (when (zero? (mod n print-size))
                     (println "indexed" n))
                   (recur (k/consume-and-index-entities indexer ek/*consumer* 100)
                          (+ n (count entities))))))))

          (t/testing "querying transacted data"
            (t/is (= #{[:http://dbpedia.org/resource/Aristotle]
                       [(keyword "http://dbpedia.org/resource/Aristotle_(painting)")]
                       [(keyword "http://dbpedia.org/resource/Aristotle_(book)")]}
                     (q/q (crux/db f/*kv*)
                          '{:find [iri]
                            :where [[e :http://xmlns.com/foaf/0.1/name "Aristotle"]
                                    [e :crux.rdf/iri iri]]})))))
      (t/is true "skipping"))))
