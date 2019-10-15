(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures.kafka :as fk]
            [crux.object-store :as os]
            [crux.lru :as lru]
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [crux.api :as api]
            [crux.tx :as tx])
  (:import java.time.Duration
           java.util.List
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.TopicPartition
           java.io.Closeable))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fk/with-kafka-client fkv/with-memdb fkv/with-kv-store)

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person {:crux.db/id "foo"}
        partitions [(TopicPartition. topic 0)]]

    (k/create-topic fk/*admin-client* topic 1 1 {})

    @(.send fk/*producer* (ProducerRecord. topic person))

    (.assign fk/*consumer* partitions)
    (let [records (.poll fk/*consumer* (Duration/ofMillis 10000))]
      (t/is (= 1 (count (seq records))))
      (t/is (= person (first (map k/consumer-record->value records)))))))

(t/deftest test-can-transact-entities
  (let [tx-topic "test-can-transact-entities-tx"
        doc-topic "test-can-transact-entities-doc"
        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/example-data-artists.nt"))
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [doc-topic])

    (db/submit-tx tx-log tx-ops)

    (let [docs (map k/consumer-record->value (.poll fk/*consumer* (Duration/ofMillis 10000)))]
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
        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (let [{:crux.tx/keys [tx-id tx-time]} @(db/submit-tx tx-log tx-ops)
            consume-opts {:indexer indexer :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}]

        (t/is (= {:txs 1 :docs 3}
                 (k/consume-and-index-entities consume-opts)))
        (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000))))

        (t/testing "restoring to stored offsets"
          (.seekToBeginning fk/*consumer* (.assignment fk/*consumer*))
          (k/seek-to-stored-offsets indexer fk/*consumer* (.assignment fk/*consumer*))
          (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000)))))

        (t/testing "querying transacted data"
          (t/is (= #{[:http://example.org/Picasso]}
                   (q/q (api/db node)
                        (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                          '{:find [e]
                            :where [[e :foaf/firstName "Pablo"]]})))))

        (t/testing "can read tx log"
          (with-open [consumer (db/new-tx-log-context tx-log)]
            (let [log (db/tx-log tx-log consumer nil)]
              (t/is (not (realized? log)))
              ;; Cannot compare the tx-ops as they contain blank nodes
              ;; with random ids.
              (t/is (= {:crux.tx/tx-time tx-time
                        :crux.tx/tx-id tx-id}
                       (dissoc (first log) :crux.tx.event/tx-events)))
              (t/is (= 1 (count log)))
              (t/is (= 3 (count (:crux.tx.event/tx-events (first log))))))))))))

(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [tx-topic "test-can-process-compacted-documents-tx"
        doc-topic "test-can-process-compacted-documents-doc"

        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))

        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})

        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)

        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (let [consume-opts {:indexer indexer
                          :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}

            evicted-doc {:crux.db/id :to-be-eviceted :personal "private"}
            non-evicted-doc {:crux.db/id :not-evicted :personal "private"}
            evicted-doc-hash
            (do @(db/submit-tx
                  tx-log
                  [[:crux.tx/put evicted-doc]
                   [:crux.tx/put non-evicted-doc]])

                (k/consume-and-index-entities consume-opts)
                (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))
                (:crux.db/content-hash (q/entity-tx (api/db node) (:crux.db/id evicted-doc))))

            after-evict-doc {:crux.db/id :after-evict :personal "private"}
            {:crux.tx/keys [tx-id tx-time]}
            (do
              @(db/submit-tx tx-log [[:crux.tx/evict (:crux.db/id evicted-doc)]])
              @(db/submit-tx
                tx-log
                [[:crux.tx/put after-evict-doc]]))]

        (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))

        (t/testing "querying transacted data"
          (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
          (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
          (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc)))))

        (t/testing "re-indexing the same transactions after doc compaction"
          (binding [fk/*consumer-options* {"max.poll.records" "1"}]
            (fk/with-kafka-client
              (fn []
                (fkv/with-kv-store
                  (fn []
                    (let [object-store (os/->KvObjectStore *kv*)
                          indexer (tx/->KvIndexer *kv* tx-log object-store nil)
                          consume-opts {:indexer indexer
                                        :consumer fk/*consumer*
                                        :pending-txs-state (atom [])
                                        :tx-topic tx-topic
                                        :doc-topic doc-topic}]
                      (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])
                      (k/consume-and-index-entities consume-opts)
                      (t/is (= {:txs 0, :docs 1} (k/consume-and-index-entities consume-opts)))
                      ;; delete the object that would have been compacted away
                      (db/delete-objects object-store [evicted-doc-hash])

                      (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))
                      (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000))))

                      (t/testing "querying transacted data"
                        (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
                        (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
                        (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc))))))))))))))))

(t/deftest test-can-transact-and-query-dbpedia-entities
  (let [tx-topic "test-can-transact-and-query-dbpedia-entities-tx"
        doc-topic "test-can-transact-and-query-dbpedia-entities-doc"
        tx-ops (->> (concat (rdf/->tx-ops (rdf/ntriples "crux/Pablo_Picasso.ntriples"))
                            (rdf/->tx-ops (rdf/ntriples "crux/Guernica_(Picasso).ntriples")))
                    (rdf/->default-language))
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (db/submit-tx tx-log tx-ops)
      (let [consume-opts {:indexer indexer :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}]
        (t/is (= {:txs 1 :docs 2}
                 (select-keys
                  (k/consume-and-index-entities
                   consume-opts)
                  [:txs :docs])))))

    (t/testing "querying transacted data"
      (t/is (= #{[:http://dbpedia.org/resource/Pablo_Picasso]}
               (q/q (api/db node)
                    (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                      '{:find [e]
                        :where [[e :foaf/givenName "Pablo"]]}))))

      (t/is (= #{[(keyword "http://dbpedia.org/resource/Guernica_(Picasso)")]}
               (q/q (api/db node)
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
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (if (and run-dbpedia-tests? (.exists mappingbased-properties-file))
      (do (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
          (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
          (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

          (t/testing "transacting and indexing"
            (future
              (time
               (with-open [in (io/input-stream mappingbased-properties-file)]
                 (reset! n-transacted (rdf/submit-ntriples tx-log in tx-size)))))
            (time
             (loop [{:keys [docs]} (k/consume-and-index-entities indexer fk/*consumer* 100)
                    n 0]
               (let [n (add-and-print-progress n docs "indexed")]
                 (when-not (= n @n-transacted)
                   (recur (k/consume-and-index-entities indexer fk/*consumer* 100)
                          (long n)))))))

          (t/testing "querying transacted data"
            (t/is (= (rdf/with-prefix {:dbr "http://dbpedia.org/resource/"}
                       #{[:dbr/Aristotle]
                         [(keyword "dbr/Aristotle_(painting)")]
                         [(keyword "dbr/Aristotle_(book)")]})
                     (q/q (api/db node)
                          (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1"})
                          '{:find [e]
                            :where [[e :foaf/name "Aristotle"]]})))))
      (t/is true "skipping"))))

;; https://jena.apache.org/tutorials/sparql.html
(t/deftest test-can-transact-and-query-using-sparql
  (let [tx-topic "test-can-transact-and-query-using-sparql-tx"
        doc-topic "test-can-transact-and-query-using-sparql-doc"
        tx-ops (->> (rdf/ntriples "crux/vc-db-1.nt") (rdf/->tx-ops) (rdf/->default-language))
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (db/submit-tx tx-log tx-ops)
      (let [consumer-opts {:indexer indexer
                           :consumer fk/*consumer*
                           :pending-txs-state (atom [])
                           :tx-topic tx-topic
                           :doc-topic doc-topic}]
        (t/is (= {:txs 1 :docs 8}
                 (k/consume-and-index-entities
                  consumer-opts)))))

    (t/testing "querying transacted data"
      (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }"))))

      (t/is (= #{[(keyword "http://somewhere/RebeccaSmith/") "Becky Smith"]
                 [(keyword "http://somewhere/SarahJones/") "Sarah Jones"]
                 [(keyword "http://somewhere/JohnSmith/") "John Smith"]
                 [(keyword "http://somewhere/MattJones/") "Matt Jones"]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
SELECT ?x ?fname
WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}"))))

      (t/is (= #{["John"]
                 ["Rebecca"]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
SELECT ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }"))))

      (t/is (= #{["Rebecca"]
                 ["Sarah"]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?g
WHERE
{ ?y vcard:Given ?g .
  FILTER regex(?g, \"r\", \"i\") }"))))

      (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
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
                 ["Sarah Jones" :crux.sparql/optional]
                 ["John Smith" 25]
                 ["Matt Jones" :crux.sparql/optional]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
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
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
PREFIX info:   <http://somewhere/peopleInfo#>
PREFIX vcard:  <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    ?person info:age ?age .
}"))))

      (t/is (= #{["Sarah Jones" :crux.sparql/optional]
                 ["John Smith" 25]
                 ["Matt Jones" :crux.sparql/optional]}
               (q/q (api/db node)
                    (sparql/sparql->datalog
                     "
PREFIX info:        <http://somewhere/peopleInfo#>
PREFIX vcard:      <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    OPTIONAL { ?person info:age ?age . FILTER ( ?age > 24 ) }
}")))))))
