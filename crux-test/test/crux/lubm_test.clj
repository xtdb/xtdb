(ns crux.lubm-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]
            [crux.fixtures.api :refer [*api*] :as apif]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.lubm :as fl]
            [crux.fixtures.kv :as kvf]
            [crux.api :as api]))

;; See:
;; http://swat.cse.lehigh.edu/projects/lubm/
;; http://swat.cse.lehigh.edu/pubs/guo05a.pdf

;; The data is generated via
;; http://swat.cse.lehigh.edu/projects/lubm/uba1.7.zip and then
;; post-processed a bit via rdf4j into a single, sorted ntriples
;; document. The University0_0.ntriples file contains the subset from
;; http://swat.cse.lehigh.edu/projects/lubm/University0_0.owl

;; TODO: most queries need rules, some assume more departments loaded.
;; Full set is available in lubm/lubm10.ntriples, but query 2 does seem to
;; be either very slow or never return, a few others also quite slow,
;; but manageable. The full set is to large to submit in a single transaction.

;; Number of triples:
;; 8519 test/lubm/University0_0.ntriples
;; 100543 test/lubm/lubm10.ntriples

;; NOTE: Test order isn't alphabetic, this can be mitigated by
;; defining a test-ns-hook (see bottom of this file) which runs the
;; tests in order, but this isn't compatible with fixtures or running
;; individual tests.
(t/use-fixtures :once
  fk/with-embedded-kafka-cluster
  fk/with-kafka-client
  fk/with-cluster-node
  kvf/with-kv-dir
  apif/with-node
  fl/with-lubm-data)

;; This query bears large input and high selectivity. It queries about just one class and
;; one property and does not assume any hierarchy information or inference.
(t/deftest test-lubm-query-01
  (t/is (= #{[:http://www.Department0.University0.edu/GraduateStudent101]
             [:http://www.Department0.University0.edu/GraduateStudent124]
             [:http://www.Department0.University0.edu/GraduateStudent142]
             [:http://www.Department0.University0.edu/GraduateStudent44]}
           (api/q (api/db *api*)
                  (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                    '{:find [x]
                      :where [[x :rdf/type :ub/GraduateStudent]
                              [x :ub/takesCourse :http://www.Department0.University0.edu/GraduateCourse0]]})))))

;; TODO: subOrganizationOf is transitive, should use rules.

;; This query increases in complexity: 3 classes and 3 properties are involved. Additionally,
;; there is a triangular pattern of relationships between the objects involved.
(t/deftest test-lubm-query-02
  (t/is (empty? (api/q (api/db *api*)
                       (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                         '{:find [x y z]
                           :where [[x :rdf/type :ub/GraduateStudent]
                                   [y :rdf/type :ub/University]
                                   [z :rdf/type :ub/Department]
                                   [x :ub/memberOf z]
                                   [z :ub/subOrganizationOf y]
                                   [x :ub/undergraduateDegreeFrom y]]})))))

;; This query is similar to Query 1 but class Publication has a wide hierarchy.
(t/deftest test-lubm-query-03
  (t/is (= #{[:http://www.Department0.University0.edu/AssistantProfessor0/Publication0]
             [:http://www.Department0.University0.edu/AssistantProfessor0/Publication1]
             [:http://www.Department0.University0.edu/AssistantProfessor0/Publication2]
             [:http://www.Department0.University0.edu/AssistantProfessor0/Publication3]
             [:http://www.Department0.University0.edu/AssistantProfessor0/Publication4]
             [:http://www.Department0.University0.edu/AssistantProfessor0/Publication5]}
           (api/q (api/db *api*)
                  (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                    '{:find [x]
                      :rules [[(sub-class-of? type root-type)
                               [(= type root-type)]]
                              [(sub-class-of? type root-type)
                               [type :rdfs/subClassOf supertype]
                               (sub-class-of? supertype root-type)]]
                      :where [[x :rdf/type t]
                              (sub-class-of? t :ub/Publication)
                              [x :ub/publicationAuthor
                               :http://www.Department0.University0.edu/AssistantProfessor0]]})))))


;; This query has small input and high selectivity. It assumes subClassOf relationship
;; between Professor and its subclasses. Class Professor has a wide hierarchy. Another
;; feature is that it queries about multiple properties of a single class.
;; Should return 34 with lubm10.ntriples.
(t/deftest test-lubm-query-04
  (let [result (api/q (api/db *api*)
                      (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                        '{:find [x y1 y2 y3]
                          :rules [[(sub-class-of? type root-type)
                                   [(= type root-type)]]
                                  [(sub-class-of? type root-type)
                                   [type :rdfs/subClassOf supertype]
                                   (sub-class-of? supertype root-type)]]
                          :where [[x :rdf/type t]
                                  (sub-class-of? t :ub/Professor)
                                  [x :ub/worksFor :http://www.Department0.University0.edu]
                                  [x :ub/name y1]
                                  [x :ub/emailAddress y2]
                                  [x :ub/telephone y3]]}))]
    (t/is (= 34 (count result)))
    #_(t/is (contains? result [:http://www.Department0.University0.edu/AssistantProfessor0
                               "AssistantProfessor0"
                               "AssistantProfessor0@Department0.University0.edu"
                               "xxx-xxx-xxxx"]))))


;; TODO: This could use rules for subPropertyOf.

;; This query assumes subClassOf relationship between Person and its subclasses
;; and subPropertyOf relationship between memberOf and its subproperties.
;; Moreover, class Person features a deep and wide hierarchy.
(t/deftest test-lubm-query-05
  (t/is (= 719 (count (api/q (api/db *api*)
                             (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                               '{:find [x]
                                 :rules [[(person? rdf-type) ; there are three root types
                                          (or [(= rdf-type :ub/Person)]
                                              [(= rdf-type :ub/Student)]
                                              [(= rdf-type :ub/Employee)])]
                                         [(person? rdf-type)
                                          [rdf-type :rdfs/subClassOf superclass]
                                          (person? superclass)]]
                                 :where [[x :rdf/type t]
                                         (person? t)
                                         ;; [x :ub/memberOf :http://www.Department0.University0.edu]
                                         (or [x :ub/memberOf :http://www.Department0.University0.edu]
                                             [x :ub/worksFor :http://www.Department0.University0.edu])]}))))))

;; TODO: Should use rules. Should return 7790 with lubm10.ntriples.

;; This query queries about only one class. But it assumes both the explicit
;; subClassOf relationship between UndergraduateStudent and Student and the
;; implicit one between GraduateStudent and Student. In addition, it has large
;; input and low selectivity.
(t/deftest test-lubm-query-06
  (t/is (= 678 (count (api/q (api/db *api*)
                             (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                               '{:find [x]
                                 :where [ ;; [x :rdf/type :ub/Student]
                                         (or [x :rdf/type :ub/Student]
                                             [x :rdf/type :ub/UndergraduateStudent]
                                             [x :rdf/type :ub/GraduateStudent])]}))))))

;; TODO: Should use rules.
;; Should return 110 with lubm10.ntriples (is this for UndergraduateStudent?).
;; EmptyHeaded returns 59 for this with UndergraduateStudent.

;; This query is similar to Query 6 in terms of class Student but it increases in the
;; number of classes and properties and its selectivity is high.
(t/deftest test-lubm-query-07
  (t/is (= 67 (count (api/q (api/db *api*)
                            (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                              '{:find [x y]
                                :where [ ;; [x :rdf/type :ub/Student]
                                        (or [x :rdf/type :ub/Student]
                                            [x :rdf/type :ub/UndergraduateStudent]
                                            [x :rdf/type :ub/GraduateStudent])
                                        ;; [y :rdf/type :ub/Course]
                                        (or [y :rdf/type :ub/Course]
                                            [y :rdf/type :ub/GraduateCourse])
                                        [x :ub/takesCourse y]
                                        [:http://www.Department0.University0.edu/AssociateProfessor0
                                         :ub/teacherOf
                                         y]]}))))))

;; TODO: Should use rules. Cannot use or for memberOf/worksFor here.
;; Should return 7790 with lubm10.ntriples (is this for UndergraduateStudent?).
;; EmptyHeaded returns 5916 for this with UndergraduateStudent.

;; This query is further more complex than Query 7 by including one more property.
(t/deftest test-lubm-query-08
  (t/is (= 678 (count (api/q (api/db *api*)
                             (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                               '{:find [x y z]
                                 :where [ ;; [x :rdf/type :ub/Student]
                                         (or [x :rdf/type :ub/Student]
                                             [x :rdf/type :ub/UndergraduateStudent]
                                             [x :rdf/type :ub/GraduateStudent])
                                         [y :rdf/type :ub/Department]
                                         [x :ub/memberOf y]
                                         [y :ub/subOrganizationOf :http://www.University0.edu]
                                         [x :ub/emailAddress z]]}))))))

;; TODO: Should use rules.
;; Should return 208 with lubm10.ntriples.

;; Besides the aforementioned features of class Student and the wide hierarchy of
;; class Faculty, like Query 2, this query is characterized by the most classes and
;; properties in the query set and there is a triangular pattern of relationships.
(t/deftest test-lubm-query-09
  (t/is (= 13 (count (api/q (api/db *api*)
                            (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                              '{:find [x y z]
                                :where [ ;; [x :rdf/type :ub/Student]
                                        (or [x :rdf/type :ub/Student]
                                            [x :rdf/type :ub/UndergraduateStudent]
                                            [x :rdf/type :ub/GraduateStudent])
                                        ;; [y :rdf/type :ub/Faculty]
                                        (or [y :rdf/type :ub/Faculty]
                                            [y :rdf/type :ub/PostDoc]
                                            [y :rdf/type :ub/Lecturer]
                                            [y :rdf/type :ub/Professor]
                                            [y :rdf/type :ub/AssistantProfessor]
                                            [y :rdf/type :ub/AssociateProfessor]
                                            [y :rdf/type :ub/Chair]
                                            [y :rdf/type :ub/Dean]
                                            [y :rdf/type :ub/FullProfessor]
                                            [y :rdf/type :ub/VisitingProfessor])
                                        ;; [z :rdf/type :ub/Course]
                                        (or [z :rdf/type :ub/Course]
                                            [z :rdf/type :ub/GraduateCourse])
                                        [x :ub/advisor y]
                                        [y :ub/teacherOf z]
                                        [x :ub/takesCourse z]]}))))))

;; TODO: Should use rules.

;; This query differs from Query 6, 7, 8 and 9 in that it only requires the
;; (implicit) subClassOf relationship between GraduateStudent and Student, i.e.,
;; subClassOf rela-tionship between UndergraduateStudent and Student does not add
;; to the results.
(t/deftest test-lubm-query-10
  (t/is (= 4 (count (api/q (api/db *api*)
                           (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                             '{:find [x]
                               :where [ ;; [x :rdf/type :ub/Student]
                                       (or [x :rdf/type :ub/Student]
                                           [x :rdf/type :ub/UndergraduateStudent]
                                           [x :rdf/type :ub/GraduateStudent])
                                       [x :ub/takesCourse :http://www.Department0.University0.edu/GraduateCourse0]]}))))))

;; TODO: should use transitive rule.
;; Should return 224 with lubm10.ntriples.

;; Query 11, 12 and 13 are intended to verify the presence of certain OWL reasoning
;; capabilities in the system. In this query, property subOrganizationOf is defined
;; as transitive. Since in the benchmark data, instances of ResearchGroup are stated
;; as a sub-organization of a Department individual and the later suborganization of
;; a University individual, inference about the subOrgnizationOf relationship between
;; instances of ResearchGroup and University is required to answer this query.
;; Additionally, its input is small.
(t/deftest test-lubm-query-11
  (t/is (= 10 (count (api/q (api/db *api*)
                            (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                              '{:find [x]
                                :where [[x :rdf/type :ub/ResearchGroup]
                                        [x :ub/subOrganizationOf d]
                                        [d :rdf/type :ub/Department]
                                        ;; [x :ub/subOrganizationOf :http://www.University0.edu]
                                        [d :ub/subOrganizationOf :http://www.University0.edu]]}))))))

;; TODO: FullProfessor should really be Chair.
;; Should return 15 with lubm10.ntriples.
;; EmptyHeaded returns 125 for this with FullProfessor.

;; The benchmark data do not produce any instances of class Chair. Instead, each
;; Department individual is linked to the chair professor of that department by
;; property headOf. Hence this query requires realization, i.e., inference that
;; that professor is an instance of class Chair because he or she is the head of a
;; department. Input of this query is small as well.
(t/deftest test-lubm-query-12
  (t/is (= 10 (count (api/q (api/db *api*)
                            (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                              '{:find [x y]
                                :where [[x :rdf/type :ub/FullProfessor]
                                        [y :rdf/type :ub/Department]
                                        [x :ub/worksFor y]
                                        [y :ub/subOrganizationOf :http://www.University0.edu]]})))))

  ;; TODO: actual result, should use rules.
  #_(t/is (= 1 (count (api/q (api/db *api*)
                             (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                               '{:find [x y]
                                 :where [[y :rdf/type :ub/Department]
                                         [x :ub/headOf y]
                                         [y :ub/subOrganizationOf :http://www.University0.edu]]}))))))

;; TODO: should use rules.

;; Property hasAlumnus is defined in the benchmark ontology as the inverse of
;; property degreeFrom, which has three subproperties: undergraduateDegreeFrom,
;; mastersDegreeFrom, and doctoralDegreeFrom. The benchmark data state a person as
;; an alumnus of a university using one of these three subproperties instead of
;; hasAlumnus. Therefore, this query assumes subPropertyOf relationships between
;; degreeFrom and its subproperties, and also requires inference about inverseOf.
(t/deftest test-lubm-query-13
  (t/is (= #{[:http://www.Department0.University0.edu/AssistantProfessor2]}
           (api/q (api/db *api*)
                  (rdf/with-prefix {:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                    :ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                    '{:find [x]
                      :where [ ;; [x :rdf/type :ub/Person]
                              (or [x :rdf/type :ub/Person]
                                  [x :rdf/type :ub/Employee]
                                  [x :rdf/type :ub/AdministrativeStaff]
                                  [x :rdf/type :ub/Faculty]
                                  [x :rdf/type :ub/PostDoc]
                                  [x :rdf/type :ub/Lecturer]
                                  [x :rdf/type :ub/Professor]
                                  [x :rdf/type :ub/AssistantProfessor]
                                  [x :rdf/type :ub/AssociateProfessor]
                                  [x :rdf/type :ub/Chair]
                                  [x :rdf/type :ub/Dean]
                                  [x :rdf/type :ub/FullProfessor]
                                  [x :rdf/type :ub/VisitingProfessor]
                                  [x :rdf/type :ub/Student]
                                  [x :rdf/type :ub/UndergraduateStudent]
                                  [x :rdf/type :ub/GraduateStudent]
                                  [x :rdf/type :ub/Director]
                                  [x :rdf/type :ub/TeachingAssistant]
                                  [x :rdf/type :ub/ResearchAssistant])
                              ;; [:http://www.University0.edu :ub/hasAlumnus x]
                              (or [x :ub/undergraduateDegreeFrom :http://www.University0.edu]
                                  [x :ub/mastersDegreeFrom :http://www.University0.edu]
                                  [x :ub/doctoralDegreeFrom :http://www.University0.edu])]})))))

;; TODO: Should return 5916 with lubm10.ntriples, which we do.

;; This query is the simplest in the test set. This query
;; represents those with large input and low selectivity and does
;; not assume any hierarchy information or inference.
(t/deftest test-lubm-query-14
  (t/is (= 532 (count (api/q (api/db *api*)
                             (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                               '{:find [x]
                                 :where [[x :rdf/type :ub/UndergraduateStudent]]}))))))
