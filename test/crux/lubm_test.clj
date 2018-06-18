(ns crux.lubm-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.tx :as tx]
            [crux.rdf :as rdf]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.fixtures :as f]
            [crux.embedded-kafka :as ek]))

(t/use-fixtures :once ek/with-embedded-kafka-cluster)
(t/use-fixtures :each ek/with-kafka-client f/with-kv-store)

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (vec (for [entity (->> (rdf/ntriples-seq in)
                           (rdf/statements->maps))]
           [:crux.tx/put (:crux.db/id entity) entity]))))

;; TODO: most queries need rules, some assume more departments loaded.
(t/deftest test-can-run-lubm-queries
  (let [tx-topic "test-can-run-lubm-queries"
        doc-topic "test-can-run-lubm-queries"
        tx-ops (->> (concat (load-ntriples-example "lubm/univ-bench.ntriples")
                            (load-ntriples-example "lubm/University0_0.ntriples"))
                    (map #(rdf/use-default-language % :en))
                    (vec))
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        indexer (tx/->DocIndexer f/*kv* tx-log (doc/->DocObjectStore f/*kv*))]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic])

    (t/testing "ensure data is indexed"
      @(db/submit-tx tx-log tx-ops)
      (while (not-empty (k/consume-and-index-entities indexer ek/*consumer*)))
      (t/testing "querying transacted data"
        (t/is (= #{[:http://www.University0.edu]}
                 (q/q (doc/db f/*kv*)
                      '{:find [u]
                        :where [[u :http://swat.cse.lehigh.edu/onto/univ-bench.owl#name "University0"]]})))))

    ;; This query bears large input and high selectivity. It queries about just one class and
    ;; one property and does not assume any hierarchy information or inference.
    (t/testing "LUBM query 1"
      (t/is (= #{[:http://www.Department0.University0.edu/GraduateStudent101]
                 [:http://www.Department0.University0.edu/GraduateStudent124]
                 [:http://www.Department0.University0.edu/GraduateStudent142]
                 [:http://www.Department0.University0.edu/GraduateStudent44]}
               (q/q (doc/db f/*kv*)
                    {:find ['x]
                     :where [['x
                              :http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse
                              :http://www.Department0.University0.edu/GraduateCourse0]
                             ['x
                              (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                              :http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent]]}))))

    ;; TODO: subOrganizationOf is transitive, should use rules.
    ;; This query increases in complexity: 3 classes and 3 properties are involved. Additionally,
    ;; there is a triangular pattern of relationships between the objects involved.
    (t/testing "LUBM query 2"
      (t/is (empty? (q/q (doc/db f/*kv*)
                         {:find ['x 'y 'z]
                          :where [['x
                                   (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent]
                                  ['y
                                   (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#University]
                                  ['z
                                   (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Department]
                                  ['x
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf
                                   'z]
                                  ['z
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf
                                   'y]
                                  ['x
                                   :http://swat.cse.lehigh.edu/onto/univ-bench.owl#undergraduateDegreeFrom
                                   'y]]}))))

    ;; TODO: Publication has subClassOf children, should use rules.
    ;; This query is similar to Query 1 but class Publication has a wide hierarchy.
    (t/testing "LUBM query 3"
      (t/is (= #{[:http://www.Department0.University0.edu/AssistantProfessor0/Publication0]
                 [:http://www.Department0.University0.edu/AssistantProfessor0/Publication1]
                 [:http://www.Department0.University0.edu/AssistantProfessor0/Publication2]
                 [:http://www.Department0.University0.edu/AssistantProfessor0/Publication3]
                 [:http://www.Department0.University0.edu/AssistantProfessor0/Publication4]
                 [:http://www.Department0.University0.edu/AssistantProfessor0/Publication5]}
               (q/q (doc/db f/*kv*)
                    {:find ['x]
                     :where [['x
                              (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                              :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Publication]
                             ['x
                              :http://swat.cse.lehigh.edu/onto/univ-bench.owl#publicationAuthor
                              :http://www.Department0.University0.edu/AssistantProfessor0]]}))))

    ;; This query has small input and high selectivity. It assumes subClassOf relationship
    ;; between Professor and its subclasses. Class Professor has a wide hierarchy. Another
    ;; feature is that it queries about multiple properties of a single class.
    #_(t/testing "LUBM query 4"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x 'y1 'y2 'y3]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Professor]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#worksFor
                                :http://www.Department0.University0.edu]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#name
                                'y1]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#emailAddress
                                'y2]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#telephone
                                'y3]]}))))


    ;; This query assumes subClassOf relationship between Person and its subclasses
    ;; and subPropertyOf relationship between memberOf and its subproperties.
    ;; Moreover, class Person features a deep and wide hierarchy.
    #_(t/testing "LUBM query 5"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf
                                :http://www.Department0.University0.edu]]}))))

    ;; This query queries about only one class. But it assumes both the explicit
    ;; subClassOf relationship between UndergraduateStudent and Student and the
    ;; implicit one between GraduateStudent and Student. In addition, it has large
    ;; input and low selectivity.
    #_(t/testing "LUBM query 6"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student]]}))))

    ;; This query is similar to Query 6 in terms of class Student but it increases in the
    ;; number of classes and properties and its selectivity is high.
    #_(t/testing "LUBM query 7"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x 'y]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student]
                               ['y
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Course]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse
                                'y]
                               [:http://www.Department0.University0.edu/AssociateProfessor0
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf
                                'y]]}))))

    ;; This query is further more complex than Query 7 by including one more property.
    #_(t/testing "LUBM query 8"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x 'y 'z]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student]
                               ['y
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Department]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf
                                'y]
                               ['y
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf
                                :http://www.University0.edu]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#emailAddress
                                'z]]}))))

    ;; Besides the aforementioned features of class Student and the wide hierarchy of
    ;; class Faculty, like Query 2, this query is characterized by the most classes and
    ;; properties in the query set and there is a triangular pattern of relationships.
    #_(t/testing "LUBM query 9"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x 'y 'z]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student]
                               ['y
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Faculty]
                               ['z
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Course]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor
                                'y]
                               ['y
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf
                                'z]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse
                                'z]]}))))
    ;; This query differs from Query 6, 7, 8 and 9 in that it only requires the
    ;; (implicit) subClassOf relationship between GraduateStudent and Student, i.e.,
    ;; subClassOf rela-tionship between UndergraduateStudent and Student does not add
    ;; to the results.
    #_(t/testing "LUBM query 10"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse
                                :http://www.Department0.University0.edu/GraduateCourse0]]}))))

    ;; Query 11, 12 and 13 are intended to verify the presence of certain OWL reasoning
    ;; capabilities in the system. In this query, property subOrganizationOf is defined
    ;; as transitive. Since in the benchmark data, instances of ResearchGroup are stated
    ;; as a sub-organization of a Department individual and the later suborganization of
    ;; a University individual, inference about the subOrgnizationOf relationship between
    ;; instances of ResearchGroup and University is required to answer this query.
    ;; Additionally, its input is small.
    #_(t/testing "LUBM query 11"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#ResearchGroup]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf
                                :http://www.University0.edu]]}))))

    ;; The benchmark data do not produce any instances of class Chair. Instead, each
    ;; Department individual is linked to the chair professor of that department by
    ;; property headOf. Hence this query requires realization, i.e., inference that
    ;; that professor is an instance of class Chair because he or she is the head of a
    ;; department. Input of this query is small as well.
    #_(t/testing "LUBM query 12"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x 'y]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Chair]
                               ['y
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Department]
                               ['x
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#worksFor
                                'y]
                               ['y
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf
                                :http://www.University0.edu]]}))))

    ;; Property hasAlumnus is defined in the benchmark ontology as the inverse of
    ;; property degreeFrom, which has three subproperties: undergraduateDegreeFrom,
    ;; mastersDegreeFrom, and doctoralDegreeFrom. The benchmark data state a person as
    ;; an alumnus of a university using one of these three subproperties instead of
    ;; hasAlumnus. Therefore, this query assumes subPropertyOf relationships between
    ;; degreeFrom and its subproperties, and also requires inference about inverseOf.
    #_(t/testing "LUBM query 13"
        (t/is (= #{}
                 (q/q (doc/db f/*kv*)
                      {:find ['x]
                       :where [['x
                                (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person]
                               [:http://www.University0.edu
                                :http://swat.cse.lehigh.edu/onto/univ-bench.owl#hasAlumnus
                                'x]]}))))

    ;; TODO: really assumes more departments loaded.
    ;; This query is the simplest in the test set. This query
    ;; represents those with large input and low selectivity and does
    ;; not assume any hierarchy information or inference.
    (t/testing "LUBM query 14"
      (t/is (= 532 (count (q/q (doc/db f/*kv*)
                               {:find ['x]
                                :where [['x
                                         (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                                         :http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent]]})))))))
