(ns xtdb.sparql-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.io :as cio]
            [xtdb.rdf :as rdf]
            [xtdb.sparql :as sparql]))

(t/deftest test-can-parse-sparql-to-datalog
  (t/testing "Apacha Jena Tutorial"
    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?x]
                 :where [[?x :vcard/FN "John Smith"]]})
             (sparql/sparql->datalog
              "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }")))

    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?y ?givenName]
                 :where [[?y :vcard/Family "Smith"]
                         [?y :vcard/Given ?givenName]]})
             (sparql/sparql->datalog
              "
SELECT ?y ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }")))

    (t/is (= (cio/pr-edn-str (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
                       '{:find [?g]
                         :where [[?y :vcard/Given ?g]
                                 [(re-find #"(?i)r" ?g)]]}))
             (cio/pr-edn-str (sparql/sparql->datalog
                      "
PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?g
WHERE
{ ?y vcard:Given ?g .
  FILTER regex(?g, \"r\", \"i\") }"))))

    (t/is (= (rdf/with-prefix {:info "http://somewhere/peopleInfo#"}
               '{:find [?resource]
                 :where [[?resource :info/age ?age]
                         [(>= ?age 24)]]})
             (sparql/sparql->datalog
              "
PREFIX info: <http://somewhere/peopleInfo#>

SELECT ?resource
WHERE
  {
    ?resource info:age ?age .
    FILTER (?age >= 24)
  }")))

    (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"
                               :vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?name]
                 :where [(or [?_anon_1 :foaf/name ?name]
                             [?_anon_2 :vcard/FN ?name])]})
             (sparql/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX vCard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name
WHERE
{
   { [] foaf:name ?name } UNION { [] vCard:FN ?name }
}")))

    (t/is (= '{:find [?name],
               :where
               [[?x #=(keyword "http://xmlns.com/foaf/0.1/givenName") ?name]
                [?x #=(keyword "http://xmlns.com/foaf/0.1/knows") ?y]
                [(== ?y #{#=(keyword "http://example.org/A") #=(keyword "http://example.org/B")})]]}
             (sparql/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE
{
  ?x foaf:givenName ?name .
  ?x foaf:knows ?y .
  FILTER(?y IN (<http://example.org/A>, <http://example.org/B>))
}")))

    (t/is (= '{:find [?name],
               :where
               [[?x #=(keyword "http://xmlns.com/foaf/0.1/givenName") ?name]
                [?x #=(keyword "http://xmlns.com/foaf/0.1/knows") ?y]
                [(!= ?y #=(keyword "http://example.org/A"))]
                [(!= ?y #=(keyword "http://example.org/B"))]]}
             (sparql/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE
{
  ?x foaf:givenName ?name .
  ?x foaf:knows ?y .
  FILTER(?y NOT IN (<http://example.org/A>, <http://example.org/B>))
}"))))

  ;; https://www.w3.org/TR/2013/REC-sparql11-query-20130321

  (t/testing "SPARQL 1.1"
    (t/is
     (= '{:find [?title],
          :where
          [[#=(keyword "http://example.org/book/book1")
            #=(keyword "http://purl.org/dc/elements/1.1/title")
            ?title]]}
        (sparql/sparql->datalog
         "SELECT ?title
WHERE
{
  <http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> ?title .
}")))

    (t/is
     (= '{:find [?name ?mbox],
          :where
          [[?x #=(keyword "http://xmlns.com/foaf/0.1/name") ?name]
           [?x #=(keyword "http://xmlns.com/foaf/0.1/mbox") ?mbox]]}
        (sparql/sparql->datalog
         "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE
  { ?x foaf:name ?name .
    ?x foaf:mbox ?mbox }")))

    (t/is (thrown-with-msg? UnsupportedOperationException #"Does not support variables in predicate position: \?p"
                            (sparql/sparql->datalog
                             "SELECT ?v WHERE { ?v ?p \"cat\"@en }")))

    (t/is
     (= '{:find [?v],
          :where
          [[?v #=(keyword "http://xmlns.com/foaf/0.1/givenName") "cat"]]}
        (sparql/sparql->datalog
         "SELECT ?v WHERE { ?v <http://xmlns.com/foaf/0.1/givenName> \"cat\"@en }")))

    (t/is
     (= '{:find [?name],
          :where
          [[?P #=(keyword "http://xmlns.com/foaf/0.1/givenName") ?G]
           [?P #=(keyword "http://xmlns.com/foaf/0.1/surname") ?S]
           [(#=(symbol "http://www.w3.org/2005/xpath-functions#concat") ?G " " ?S) ?name]]}
        (sparql/sparql->datalog
         "
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE  {
   ?P foaf:givenName ?G ;
      foaf:surname ?S
   BIND(CONCAT(?G, \" \", ?S) AS ?name)
}")))

    (t/is
     (= '{:find [?name],
          :where
          [[?P #=(keyword "http://xmlns.com/foaf/0.1/givenName") ?G]
           [?P #=(keyword "http://xmlns.com/foaf/0.1/surname") ?S]
           [(#=(symbol "http://www.w3.org/2005/xpath-functions#concat") ?G " " ?S) ?name]]}
        (sparql/sparql->datalog
         "
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ( CONCAT(?G, \" \", ?S) AS ?name )
WHERE  { ?P foaf:givenName ?G ; foaf:surname ?S }
")))

    (t/is (= (cio/pr-edn-str '{:find [?title],
                       :where
                       [[?x #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                        [(re-find #"^SPARQL" ?title)]]})
             (cio/pr-edn-str (sparql/sparql->datalog
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"^SPARQL\")
        }"))))

    (t/is (= (cio/pr-edn-str '{:find [?title],
                       :where
                       [[?x #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                        [(re-find #"(?i)web" ?title)]]})
             (cio/pr-edn-str (sparql/sparql->datalog
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"web\", \"i\" )
        }"))))

    (t/is (= '{:find [?title ?price],
               :where
               [[?x #=(keyword "http://example.org/ns#price") ?price]
                [?x #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                [(< ?price 30.5M)]]}
             (sparql/sparql->datalog
              "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>
SELECT  ?title ?price
WHERE   { ?x ns:price ?price .
          FILTER (?price < 30.5)
          ?x dc:title ?title . }")))

    (t/is (= '{:find [?name ?mbox],
               :where
               [[?x #=(keyword "http://xmlns.com/foaf/0.1/name") ?name]
                (or-join
                 [?mbox ?x]
                 [?x #=(keyword "http://xmlns.com/foaf/0.1/mbox") ?mbox]
                 (and [(identity :xtdb.sparql/optional) ?mbox]
                      (not [?x #=(keyword "http://xmlns.com/foaf/0.1/mbox")])))]}
             (sparql/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE  { ?x foaf:name  ?name .
         OPTIONAL { ?x  foaf:mbox  ?mbox }
       }")))

    (t/is (= '{:find [?title ?price],
               :where
               [[?x #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                (or-join
                 [?x ?price]
                 (and [?x #=(keyword "http://example.org/ns#price") ?price] [(< ?price 30)])
                 (and (not [?x #=(keyword "http://example.org/ns#price")])
                      [(identity :xtdb.sparql/optional) ?price]))]}
             (sparql/sparql->datalog "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>
SELECT  ?title ?price
WHERE   { ?x dc:title ?title .
          OPTIONAL { ?x ns:price ?price . FILTER (?price < 30) }
        }")))

    (t/is (= '{:find [?title],
               :where
               [(or
                 [?book #=(keyword "http://purl.org/dc/elements/1.0/title") ?title]
                 [?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?title])]}
             (sparql/sparql->datalog "
PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
PREFIX dc11:  <http://purl.org/dc/elements/1.1/>

SELECT ?title
WHERE  { { ?book dc10:title  ?title } UNION { ?book dc11:title  ?title } }")))

    ;; TODO: this should really be working like optional and select
    ;; both ?x and ?y and not ?book
    (t/is (= '{:find [?book],
               :where
               [(or-join [?book]
                         [?book #=(keyword "http://purl.org/dc/elements/1.0/title") ?x]
                         [?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?y])]}
             (sparql/sparql->datalog "
PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
PREFIX dc11:  <http://purl.org/dc/elements/1.1/>

SELECT ?book
WHERE  { { ?book dc10:title ?x } UNION { ?book dc11:title  ?y } }")))


    (t/is (= '{:find [?title ?author],
               :where
               [(or (and [?book #=(keyword "http://purl.org/dc/elements/1.0/title") ?title]
                         [?book #=(keyword "http://purl.org/dc/elements/1.0/creator") ?author])
                    (and [?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                         [?book #=(keyword "http://purl.org/dc/elements/1.1/creator") ?author]))]}
             (sparql/sparql->datalog "
PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
PREFIX dc11:  <http://purl.org/dc/elements/1.1/>

SELECT ?title ?author
WHERE  { { ?book dc10:title ?title .  ?book dc10:creator ?author }
         UNION
         { ?book dc11:title ?title .  ?book dc11:creator ?author }
       }")))

    (t/is (= (rdf/with-prefix
               '{:find [?person],
                 :where
                 [[?person
                   :rdf/type
                   #=(keyword "http://xmlns.com/foaf/0.1/Person")]
                  (not-join [?person] [?person #=(keyword "http://xmlns.com/foaf/0.1/name") ?name])]})
             (sparql/sparql->datalog "
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>

SELECT ?person
WHERE
{
    ?person rdf:type  foaf:Person .
    FILTER NOT EXISTS { ?person foaf:name ?name }
}")))

    (t/is (= (rdf/with-prefix
               '{:find [?person],
                 :where
                 [[?person
                   :rdf/type
                   #=(keyword "http://xmlns.com/foaf/0.1/Person")]
                  [?person #=(keyword "http://xmlns.com/foaf/0.1/name") ?name]]})
             (sparql/sparql->datalog "
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>

SELECT ?person
WHERE
{
    ?person rdf:type  foaf:Person .
    FILTER EXISTS { ?person foaf:name ?name }
}")))

    ;; NOTE: original has DISTINCT in select and ?p as predicate.
    (t/is (thrown-with-msg? UnsupportedOperationException #"MINUS not supported, use NOT EXISTS."
                            (sparql/sparql->datalog
                             "              PREFIX :       <http://example/>
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>

SELECT ?s
WHERE {
   ?s foaf:givenName ?o .
   MINUS {
      ?s foaf:givenName \"Bob\" .
   }
}")))

    ;; NOTE: Adapted to remove first rdf:type/ part of the path which
    ;; simply expands to a blank node with a random id.
    (t/is (= '{:find [?x ?type],
               :where [(#=(symbol "http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR") ?x ?type)]
               :rules [[(#=(symbol "http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR") ?s ?o)
                        [?s #=(keyword "http://www.w3.org/2000/01/rdf-schema#subClassOf") ?o]]
                       [(#=(symbol "http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR") ?s ?o)
                        [?s #=(keyword "http://www.w3.org/2000/01/rdf-schema#subClassOf") ?t]
                        (#=(symbol "http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR") ?t ?o)]
                       [(#=(symbol "http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR") ?s ?o)
                        [?s :xt/id]
                        [(identity :xtdb.sparql/zero-matches) ?o]]]}
             (sparql/sparql->datalog "
PREFIX  rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?x ?type
{
  ?x  rdfs:subClassOf* ?type
}")))

    (t/is (= '{:find [?person],
               :where [(#=(symbol "http://xmlns.com/foaf/0.1/knows-PLUS") #=(keyword "http://example/x") ?person)]
               :rules [[(#=(symbol "http://xmlns.com/foaf/0.1/knows-PLUS") ?s ?o)
                        [?s #=(keyword "http://xmlns.com/foaf/0.1/knows") ?o]]
                       [(#=(symbol "http://xmlns.com/foaf/0.1/knows-PLUS") ?s ?o)
                        [?s #=(keyword "http://xmlns.com/foaf/0.1/knows") ?t]
                        (#=(symbol "http://xmlns.com/foaf/0.1/knows-PLUS") ?t ?o)]]}
             (sparql/sparql->datalog "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX :     <http://example/>
SELECT ?person
{
  :x foaf:knows+ ?person
}")))

    ;; NOTE: Adapted from above example for zero-or-one.
    ;; Parses to distinct and ZeroLengthPath in a union.
    (t/is (= '{:find [?person],
               :where
               [(or-join [?person]
                         (and [#=(keyword "http://example/x") :xt/id]
                              [(identity :xtdb.sparql/zero-matches) ?person])
                         [#=(keyword "http://example/x") #=(keyword "http://xmlns.com/foaf/0.1/knows") ?person])]}
             (sparql/sparql->datalog "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX :     <http://example/>
SELECT ?person
{
  :x foaf:knows? ?person
}")))

    (t/is (thrown-with-msg? UnsupportedOperationException #"Nested expressions are not supported."
                            (sparql/sparql->datalog "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>

SELECT  ?title ?price
{  { ?x ns:price ?p .
     ?x ns:discount ?discount
     BIND (?p*(1-?discount) AS ?price)
   }
   {?x dc:title ?title . }
   FILTER(?price < 20)
}
")))

    (t/is (= '{:find [?book ?title ?price],
               :where
               [[?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                [?book #=(keyword "http://example.org/ns#price") ?price]],
               :args
               [{?book #=(keyword "http://example.org/book/book1")}
                {?book #=(keyword "http://example.org/book/book3")}]}
             (sparql/sparql->datalog "
PREFIX dc:   <http://purl.org/dc/elements/1.1/>
PREFIX :     <http://example.org/book/>
PREFIX ns:   <http://example.org/ns#>

SELECT ?book ?title ?price
{
   VALUES ?book { :book1 :book3 }
   ?book dc:title ?title ;
         ns:price ?price .
}")))

    (t/is (= '{:find [?book ?title ?price],
               :where
               [[?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                [?book #=(keyword "http://example.org/ns#price") ?price]],
               :args
               [{?book :xtdb.sparql/undefined, ?title "SPARQL Tutorial"}
                {?book #=(keyword "http://example.org/book/book2"), ?title :xtdb.sparql/undefined}]}
             (sparql/sparql->datalog "
PREFIX dc:   <http://purl.org/dc/elements/1.1/>
PREFIX :     <http://example.org/book/>
PREFIX ns:   <http://example.org/ns#>

SELECT ?book ?title ?price
{
   ?book dc:title ?title ;
         ns:price ?price .
   VALUES (?book ?title)
   { (UNDEF \"SPARQL Tutorial\")
     (:book2 UNDEF) } }")))

    (t/is (= '{:find [?book ?title ?price],
               :where
               [[?book #=(keyword "http://purl.org/dc/elements/1.1/title") ?title]
                [?book #=(keyword "http://example.org/ns#price") ?price]],
               :args
               [{?book :xtdb.sparql/undefined, ?title "SPARQL Tutorial"}
                {?book #=(keyword "http://example.org/book/book2") ?title :xtdb.sparql/undefined}]}
             (sparql/sparql->datalog "
PREFIX dc:   <http://purl.org/dc/elements/1.1/>
PREFIX :     <http://example.org/book/>
PREFIX ns:   <http://example.org/ns#>

SELECT ?book ?title ?price
{
   ?book dc:title ?title ;
         ns:price ?price .
}
VALUES (?book ?title)
{ (UNDEF \"SPARQL Tutorial\")
  (:book2 UNDEF)
}")))

    (t/is (= '{:find [?name],
               :where [[?x #=(keyword "http://xmlns.com/foaf/0.1/name") ?name]]
               :limit 20
               :order-by [[?name :asc]]}
             (sparql/sparql->datalog
              "
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>

SELECT ?name
WHERE { ?x foaf:name ?name }
ORDER BY ?name
LIMIT 20
")))

    (t/is (= (rdf/with-prefix {:wsdbm "http://db.uwaterloo.ca/~galuc/wsdbm/"}
               '{:find [?v0 ?v1 ?v5 ?v2 ?v3]
                 :where [[?v0 :wsdbm/gender :wsdbm/Gender1]
                         [?v0 #=(keyword "http://purl.org/dc/terms/Location") ?v1]
                         [?v0 :wsdbm/follows ?v0]
                         [?v0 :wsdbm/userId ?v5]
                         [?v1 #=(keyword "http://www.geonames.org/ontology#parentCountry") ?v2]
                         [?v3 #=(keyword "http://purl.org/ontology/mo/performed_in") ?v1]]})
             (sparql/sparql->datalog "
SELECT * WHERE {
   ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
   ?v0 <http://purl.org/dc/terms/Location> ?v1 .
   ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0 .
   ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5 .
   ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2 .
   ?v3 <http://purl.org/ontology/mo/performed_in> ?v1 .
 }")))))
