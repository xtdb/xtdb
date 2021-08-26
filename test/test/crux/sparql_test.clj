(ns crux.sparql-test
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.api :as xt]
            [clojure.java.io :as io]
            [xtdb.sparql :as sparql]
            [xtdb.rdf :as rdf]))

(t/use-fixtures :each fix/with-node)

;; https://jena.apache.org/tutorials/sparql.html
(t/deftest test-can-transact-and-query-using-sparql
  (fix/submit+await-tx (->> (rdf/ntriples "crux/vc-db-1.nt") (rdf/->tx-ops) (rdf/->default-language)))

  (t/testing "querying transacted data"
    (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
             (xt/q (xt/db *api*)
                     (sparql/sparql->datalog
                      "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }"))))

    (t/is (= #{[(keyword "http://somewhere/RebeccaSmith/") "Becky Smith"]
               [(keyword "http://somewhere/SarahJones/") "Sarah Jones"]
               [(keyword "http://somewhere/JohnSmith/") "John Smith"]
               [(keyword "http://somewhere/MattJones/") "Matt Jones"]}
             (xt/q (xt/db *api*)
                     (sparql/sparql->datalog
                      "
SELECT ?x ?fname
WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}"))))

    (t/is (= #{["John"]
               ["Rebecca"]}
             (xt/q (xt/db *api*)
                     (sparql/sparql->datalog
                      "
SELECT ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }"))))

    (t/is (= #{["Rebecca"]
               ["Sarah"]}
             (xt/q (xt/db *api*)
                     (sparql/sparql->datalog
                      "
PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?g
WHERE
{ ?y vcard:Given ?g .
  FILTER regex(?g, \"r\", \"i\") }"))))

    (t/is (= #{[(keyword "http://somewhere/JohnSmith/")]}
             (xt/q (xt/db *api*)
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
               ["Sarah Jones" :xtdb.sparql/optional]
               ["John Smith" 25]
               ["Matt Jones" :xtdb.sparql/optional]}
             (xt/q (xt/db *api*)
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
             (xt/q (xt/db *api*)
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

    (t/is (= #{["Sarah Jones" :xtdb.sparql/optional]
               ["John Smith" 25]
               ["Matt Jones" :xtdb.sparql/optional]}
             (xt/q (xt/db *api*)
                     (sparql/sparql->datalog
                      "
PREFIX info:        <http://somewhere/peopleInfo#>
PREFIX vcard:      <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name ?age
WHERE
{
    ?person vcard:FN  ?name .
    OPTIONAL { ?person info:age ?age . FILTER ( ?age > 24 ) }
}"))))))
