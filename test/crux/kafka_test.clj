(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.tx :as tx]
            [crux.rdf :as rdf]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.fixtures :as f]
            [crux.embedded-kafka :as ek])
  (:import [java.time Duration]
           [org.apache.kafka.clients.producer
            ProducerRecord]
           [org.apache.kafka.common TopicPartition]))

(t/use-fixtures :once ek/with-embedded-kafka-cluster)
(t/use-fixtures :each ek/with-kafka-client f/with-kv-store)

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person (f/random-person)
        partitions [(TopicPartition. topic 0)]]

    (k/create-topic ek/*admin-client* topic 1 1 {})

    @(.send ek/*producer* (ProducerRecord. topic person))

    (.assign ek/*consumer* partitions)
    (let [records (.poll ek/*consumer* (Duration/ofMillis 10000))]
      (t/is (= 1 (count (seq records))))
      (t/is (= person (first (map k/consumer-record->value records)))))))

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (vec (for [entity (->> (rdf/ntriples-seq in)
                           (rdf/statements->maps))]
           [:crux.tx/put (:crux.db/id entity) entity]))))

(t/deftest test-can-transact-entities
  (let [tx-topic "test-can-transact-entities-tx"
        doc-topic "test-can-transact-entities-doc"
        tx-ops (load-ntriples-example  "crux/example-data-artists.nt")
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [doc-topic])

    (db/submit-tx tx-log tx-ops)

    (let [docs (map k/consumer-record->value (.poll ek/*consumer* (Duration/ofMillis 10000)))]
      (t/is (= 7 (count docs)))
      (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                 {:foaf/firstName "Pablo"
                  :foaf/surname "Picasso"})
               (select-keys (first docs)
                            (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                              [:foaf/firstName
                               :foaf/surname])))))))

(t/deftest test-can-transact-and-query-entities
  (let [tx-topic "test-can-transact-and-query-entities-tx"
        doc-topic "test-can-transact-and-query-entities-doc"
        tx-ops (load-ntriples-example  "crux/picasso.nt")
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (db/submit-tx tx-log tx-ops)

      (t/is (= {:txs 1 :docs 3} (k/consume-and-index-entities indexer ek/*consumer*)))
      (t/is (empty? (.poll ek/*consumer* (Duration/ofMillis 1000)))))

    (t/testing "restoring to stored offsets"
      (.seekToBeginning ek/*consumer* (.assignment ek/*consumer*))
      (k/seek-to-stored-offsets indexer ek/*consumer* (.assignment ek/*consumer*))
      (t/is (empty? (.poll ek/*consumer* (Duration/ofMillis 1000)))))

    (t/testing "querying transacted data"
      (t/is (= #{[:http://example.org/Picasso]}
               (q/q (q/db f/*kv*)
                    (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                      '{:find [e]
                        :where [[e :foaf/firstName "Pablo"]]})))))))

(t/deftest test-can-transact-and-query-dbpedia-entities
  (let [tx-topic "test-can-transact-and-query-dbpedia-entities-tx"
        doc-topic "test-can-transact-and-query-dbpedia-entities-doc"
        tx-ops (->> (concat (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                            (load-ntriples-example "crux/Guernica_(Picasso).ntriples"))
                    (map #(rdf/use-default-language % :en))
                    (vec))
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (db/submit-tx tx-log tx-ops)
      (t/is (= {:txs 1 :docs 2} (k/consume-and-index-entities indexer ek/*consumer*))))

    (t/testing "querying transacted data"
      (t/is (= #{[:http://dbpedia.org/resource/Pablo_Picasso]}
               (q/q (q/db f/*kv*)
                    (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                      '{:find [e]
                        :where [[e :foaf/givenName "Pablo"]]}))))

      (t/is (= #{[(keyword "http://dbpedia.org/resource/Guernica_(Picasso)")]}
               (q/q (q/db f/*kv*)
                    (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"
                                      :dbo "http://dbpedia.org/ontology/"}
                      '{:find [g]
                        :where [[p :foaf/givenName "Pablo"]
                                [g :dbo/author p]]})))))))

;; Download from http://wiki.dbpedia.org/services-resources/ontology
;; mappingbased_properties_en.nt is the main data.
;; instance_types_en.nt contains type definitions only.
;; specific_mappingbased_properties_en.nt contains extra literals.
;; dbpedia_2014.owl is the OWL schema, not dealt with.

;; Test assumes these files are living under ../dbpedia related to the
;; crux project (to change).

;; There are 5053979 entities across 33449633 triplets in
;; mappingbased_properties_en.nt.

;; RocksDB:
;; 1.6G    /tmp/kafka-log1572248494326726941
;; 2.0G    /tmp/kv-store8136342842355297151
;; 583800ms ~9.7mins transact
;; 861799ms ~14.40mins index

;; LMDB:
;; 1.6G    /tmp/kafka-log17904986480319416547
;; 9.3G    /tmp/kv-store4104462813030460112
;; 640528ms ~10.7mins transact
;; 2940230ms 49mins index

;; Could use test selectors.
(def run-dbpedia-tests? false)

(t/deftest test-can-transact-all-dbpedia-entities
  (let [tx-topic "test-can-transact-all-dbpedia-entities-tx"
        doc-topic "test-can-transact-all-dbpedia-entities-doc"
        tx-size 1000
        max-limit Long/MAX_VALUE
        print-size 100000
        add-and-print-progress (fn [n step message]
                                 (let [next-n (+ n step)]
                                   (when-not (= (quot n print-size)
                                                (quot next-n print-size))
                                     (log/warn message next-n))
                                   next-n))
        n-transacted (atom -1)
        mappingbased-properties-file (io/file "../dbpedia/mappingbased_properties_en.nt")
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (if (and run-dbpedia-tests? (.exists mappingbased-properties-file))
      (do (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
          (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
          (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic])

          (t/testing "transacting and indexing"
            (future
              (time
               (with-open [in (io/input-stream mappingbased-properties-file)]
                 (reset! n-transacted (rdf/submit-ntriples tx-log in tx-size)))))
            (time
             (loop [{:keys [docs]} (k/consume-and-index-entities indexer ek/*consumer* 100)
                    n 0]
               (let [n (add-and-print-progress n docs "indexed")]
                 (when-not (= n @n-transacted)
                   (recur (k/consume-and-index-entities indexer ek/*consumer* 100)
                          (long n)))))))

          (t/testing "querying transacted data"
            (t/is (= (rdf/with-prefix {:dbr "http://dbpedia.org/resource/"}
                       #{[:dbr/Aristotle]
                         [(keyword "dbr/Aristotle_(painting)")]
                         [(keyword "dbr/Aristotle_(book)")]})
                     (q/q (q/db f/*kv*)
                          (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1"})
                          '{:find [e]
                            :where [[e :foaf/name "Aristotle"]]})))))
      (t/is true "skipping"))))

;; https://jena.apache.org/tutorials/sparql.html
(t/deftest test-can-transact-and-query-using-sparql
  (let [tx-topic "test-can-transact-and-query-using-sparql-tx"
        doc-topic "test-can-transact-and-query-using-sparql-doc"
        tx-ops (->> (load-ntriples-example "crux/vc-db-1.nt")
                    (map #(rdf/use-default-language % :en))
                    (vec))
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (db/submit-tx tx-log tx-ops)
      (t/is (= {:txs 1 :docs 8} (k/consume-and-index-entities indexer ek/*consumer*))))

    (t/testing "querying transacted data"
      (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }"))))

      (t/is (= #{[(keyword "http://somewhere/RebeccaSmith/") "Becky Smith"]
                 [(keyword "http://somewhere/SarahJones/") "Sarah Jones"]
                 [(keyword "http://somewhere/JohnSmith/") "John Smith"]
                 [(keyword "http://somewhere/MattJones/") "Matt Jones"]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
SELECT ?x ?fname
WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}"))))

      (t/is (= #{["John"]
                 ["Rebecca"]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
SELECT ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }"))))

      (t/is (= #{["Rebecca"]
                 ["Sarah"]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?g
WHERE
{ ?y vcard:Given ?g .
  FILTER regex(?g, \"r\", \"i\") }"))))

      (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
PREFIX info: <http://somewhere/peopleInfo#>

SELECT ?resource
WHERE
  {
    ?resource info:age ?age .
    FILTER (?age >= 24)
  }"))))

      ;; NOTE: Without post processing the extra optional is correct.
      (t/is (= #{["Becky Smith" 23]
                 ["Becky Smith" :crux.rdf/optional]
                 ["Sarah Jones" :crux.rdf/optional]
                 ["John Smith" 25]
                 ["John Smith" :crux.rdf/optional]
                 ["Matt Jones" :crux.rdf/optional]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
PREFIX info:    <http://somewhere/peopleInfo#>
PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    OPTIONAL { ?person info:age ?age }
}"))))

      (t/is (= #{["Becky Smith" 23]
                 ["John Smith" 25]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
PREFIX info:   <http://somewhere/peopleInfo#>
PREFIX vcard:  <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    ?person info:age ?age .
}"))))

      (t/is (= #{["Becky Smith" :crux.rdf/optional]
                 ["Sarah Jones" :crux.rdf/optional]
                 ["John Smith" 25]
                 ["John Smith" :crux.rdf/optional]
                 ["Matt Jones" :crux.rdf/optional]}
               (q/q (q/db f/*kv*)
                    (crux.rdf/sparql->datalog
              "
PREFIX info:        <http://somewhere/peopleInfo#>
PREFIX vcard:      <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    OPTIONAL { ?person info:age ?age . FILTER ( ?age > 24 ) }
}")))))))
