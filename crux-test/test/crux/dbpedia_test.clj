(ns crux.dbpedia-test
  (:require [clojure.test :as t]))

#_(t/use-fixtures :once fk/with-embedded-kafka-cluster)
#_(t/use-fixtures :each with-each-api-implementation kvf/with-kv-dir apif/with-node)

#_(t/deftest test-can-transact-and-query-dbpedia-entities
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

#_(t/deftest test-can-transact-all-dbpedia-entities
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
                 (reset! n-transacted (:entity-count (rdf/submit-ntriples tx-log in tx-size))))))
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
#_(t/deftest test-can-transact-and-query-using-sparql
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
