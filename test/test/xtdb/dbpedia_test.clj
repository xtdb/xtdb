(ns xtdb.dbpedia-test
  (:require [clojure.test :as t]
            [xtdb.fixtures.kafka :as fk]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.api :as xt]
            [xtdb.rdf :as rdf]
            [clojure.java.io :as io]
            [xtdb.sparql :as sparql]))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fk/with-cluster-tx-log-opts fk/with-cluster-doc-store-opts fix/with-node)

(t/deftest test-can-transact-and-query-dbpedia-entities
  (fix/submit+await-tx (->> (concat (rdf/->tx-ops (rdf/ntriples "xtdb/Pablo_Picasso.ntriples"))
                                    (rdf/->tx-ops (rdf/ntriples "xtdb/Guernica_(Picasso).ntriples")))
                            (rdf/->default-language)))

  (t/is (= #{[(keyword "http://dbpedia.org/resource/Pablo_Picasso")]}
           (xt/q (xt/db *api*)
                   (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                     '{:find [e]
                       :where [[e :foaf/givenName "Pablo"]]}))))

  (t/is (= #{[(keyword "http://dbpedia.org/resource/Guernica_(Picasso)")]}
           (xt/q (xt/db *api*)
                   (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"
                                     :dbo "http://dbpedia.org/ontology/"}
                     '{:find [g]
                       :where [[p :foaf/givenName "Pablo"]
                               [g :dbo/author p]]})))))

;; Download from http://wiki.dbpedia.org/services-resources/ontology
;; mappingbased_properties_en.nt is the main data.
;; instance_types_en.nt contains type definitions only.
;; specific_mappingbased_properties_en.nt contains extra literals.
;; dbpedia_2014.owl is the OWL schema, not dealt with.

;; Test assumes these files are living somewhere on the classpath
;; (probably crux-test/resources related to the crux project).

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
  (let [mappingbased-properties-file (io/resource "dbpedia/mappingbased_properties_en.nt")]
    (if (and run-dbpedia-tests? mappingbased-properties-file)
      (let [max-limit Long/MAX_VALUE]
        (t/testing "ingesting data"
          (time
           (rdf/with-ntriples mappingbased-properties-file
             (fn [ntriples]
               (let [last-tx (->> ntriples
                                  (map rdf/->tx-op)
                                  (take max-limit)
                                  (partition-all 1000)
                                  (reduce (fn [_ ops]
                                            (xt/submit-tx *api* ops))))]
                 (xt/await-tx *api* last-tx))))))

        (t/testing "querying transacted data"
          (t/is (= (rdf/with-prefix {:dbr "http://dbpedia.org/resource/"}
                     #{[:dbr/Aristotle]
                       [(keyword "dbr/Aristotle_(painting)")]
                       [(keyword "dbr/Aristotle_(book)")]})
                   (xt/q (xt/db *api*)
                           (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1"}
                             '{:find [e]
                               :where [[e :foaf/name "Aristotle"]]}))))))

      (t/is true "skipping"))))
