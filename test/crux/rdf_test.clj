(ns crux.rdf-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]))

(defn load-ntriples-example [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (rdf/maps-by-id))))

;; Example based on:
;; https://github.com/eclipse/rdf4j-doc/blob/master/examples/src/main/resources/example-data-artists.ttl
(t/deftest test-can-parse-ntriples-into-maps
  (let [iri->entity (load-ntriples-example "crux/example-data-artists.nt")]
    (t/is (= 7 (count iri->entity)))

    (let [artist (:http://example.org/Picasso iri->entity)
          painting (:http://example.org/creatorOf artist)]

      (t/is (= :http://example.org/guernica painting))
      (t/is (= "oil on canvas"
               (-> painting
                   iri->entity
                   :http://example.org/technique)))

      (t/is (= {:http://example.org/street "31 Art Gallery",
                :http://example.org/city "Madrid",
                :http://example.org/country "Spain"}
               (-> artist
                   :http://example.org/homeAddress
                   iri->entity
                   (dissoc :crux.db/id)))))))

(t/deftest test-can-parse-dbpedia-entity
  (let [picasso (-> (load-ntriples-example "crux/Pablo_Picasso.ntriples")
                    :http://dbpedia.org/resource/Pablo_Picasso)]
    (t/is (= 48 (count picasso)))
    (t/is (= {:http://xmlns.com/foaf/0.1/givenName #crux.rdf.Lang{:en "Pablo"}
              :http://xmlns.com/foaf/0.1/surname #crux.rdf.Lang{:en "Picasso"}
              :http://dbpedia.org/ontology/birthDate #inst "1881-10-25"}
             (select-keys picasso
                          [:http://xmlns.com/foaf/0.1/givenName
                           :http://xmlns.com/foaf/0.1/surname
                           :http://dbpedia.org/ontology/birthDate])))

    (t/is (= {:http://xmlns.com/foaf/0.1/givenName "Pablo"
              :http://xmlns.com/foaf/0.1/surname "Picasso"}
             (select-keys (rdf/use-default-language picasso :en)
                          [:http://xmlns.com/foaf/0.1/givenName
                           :http://xmlns.com/foaf/0.1/surname])))))

(t/deftest test-can-parse-sparql-to-datalog
  (t/testing "Apacha Jena Tutorial"
    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?x]
                 :where [[?x :vcard/FN "John Smith"]]})
             (rdf/sparql->datalog
              "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }")))

    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?y ?givenName]
                 :where [[?y :vcard/Family "Smith"]
                         [?y :vcard/Given ?givenName]]})
             (rdf/sparql->datalog
              "
SELECT ?y ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }")))

    (t/is (= (pr-str (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
                       '{:find [?g]
                         :where [[?y :vcard/Given ?g]
                                 [(re-find #"(?i)r" ?g)]]}))
             (pr-str (rdf/sparql->datalog
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
             (rdf/sparql->datalog
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
             (rdf/sparql->datalog
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
               [[?x :http://xmlns.com/foaf/0.1/givenName ?name]
                [?x :http://xmlns.com/foaf/0.1/knows ?y]
                [(== ?y #{:http://example.org/A :http://example.org/B})]]}
             (rdf/sparql->datalog
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
               [[?x :http://xmlns.com/foaf/0.1/givenName ?name]
                [?x :http://xmlns.com/foaf/0.1/knows ?y]
                [(!= ?y :http://example.org/A)]
                [(!= ?y :http://example.org/B)]]}
             (rdf/sparql->datalog
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
          [[:http://example.org/book/book1
            :http://purl.org/dc/elements/1.1/title
            ?title]]}
        (rdf/sparql->datalog
         "SELECT ?title
WHERE
{
  <http://example.org/book/book1> <http://purl.org/dc/elements/1.1/title> ?title .
}")))

    (t/is
     (= '{:find [?name ?mbox],
          :where
          [[?x :http://xmlns.com/foaf/0.1/name ?name]
           [?x :http://xmlns.com/foaf/0.1/mbox ?mbox]]}
        (rdf/sparql->datalog
         "PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE
  { ?x foaf:name ?name .
    ?x foaf:mbox ?mbox }")))

    (t/is (thrown-with-msg? UnsupportedOperationException #"Does not support variables in predicate position: \?p"
                            (rdf/sparql->datalog
                             "SELECT ?v WHERE { ?v ?p \"cat\"@en }")))

    (t/is
     (= '{:find [?v],
          :where
          [[?v :http://xmlns.com/foaf/0.1/givenName "cat"]]}
        (rdf/sparql->datalog
         "SELECT ?v WHERE { ?v <http://xmlns.com/foaf/0.1/givenName> \"cat\"@en }")))

    (t/is
     (= '{:find [?name],
          :where
          [[?P :http://xmlns.com/foaf/0.1/givenName ?G]
           [?P :http://xmlns.com/foaf/0.1/surname ?S]
           [(http://www.w3.org/2005/xpath-functions#concat ?G " " ?S) ?name]]}
        (rdf/sparql->datalog
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
          [[?P :http://xmlns.com/foaf/0.1/givenName ?G]
           [?P :http://xmlns.com/foaf/0.1/surname ?S]
           [(http://www.w3.org/2005/xpath-functions#concat ?G " " ?S) ?name]]}
        (rdf/sparql->datalog
         "
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ( CONCAT(?G, \" \", ?S) AS ?name )
WHERE  { ?P foaf:givenName ?G ; foaf:surname ?S }
")))

    (t/is (= (pr-str '{:find [?title],
                       :where
                       [[?x :http://purl.org/dc/elements/1.1/title ?title]
                        [(re-find #"^SPARQL" ?title)]]})
             (pr-str (rdf/sparql->datalog
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"^SPARQL\")
        }"))))

    (t/is (= (pr-str '{:find [?title],
                       :where
                       [[?x :http://purl.org/dc/elements/1.1/title ?title]
                        [(re-find #"(?i)web" ?title)]]})
             (pr-str (rdf/sparql->datalog
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"web\", \"i\" )
        }"))))

    (t/is (= '{:find [?title ?price],
               :where
               [[?x :http://example.org/ns#price ?price]
                [?x :http://purl.org/dc/elements/1.1/title ?title]
                [(< ?price 30.5M)]]}
             (rdf/sparql->datalog
              "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>
SELECT  ?title ?price
WHERE   { ?x ns:price ?price .
          FILTER (?price < 30.5)
          ?x dc:title ?title . }")))

    (t/is (= '{:find [?name ?mbox],
               :where
               [[?x :http://xmlns.com/foaf/0.1/name ?name]
                (or-join
                 [?mbox ?x]
                 [?x :http://xmlns.com/foaf/0.1/mbox ?mbox]
                 (and [(identity :crux.rdf/optional) ?mbox] [(identity ?x)]))]}
             (rdf/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE  { ?x foaf:name  ?name .
         OPTIONAL { ?x  foaf:mbox  ?mbox }
       }")))

    (t/is (= '{:find [?title ?price],
               :where
               [[?x :http://purl.org/dc/elements/1.1/title ?title]
                (or-join
                 [?x ?price]
                 (and [?x :http://example.org/ns#price ?price] [(< ?price 30)])
                 (and [(identity ?x)] [(identity :crux.rdf/optional) ?price]))]}
             (rdf/sparql->datalog "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>
SELECT  ?title ?price
WHERE   { ?x dc:title ?title .
          OPTIONAL { ?x ns:price ?price . FILTER (?price < 30) }
        }")))

    (t/is (= '{:find [?title],
               :where
               [(or
                 [?book :http://purl.org/dc/elements/1.0/title ?title]
                 [?book :http://purl.org/dc/elements/1.1/title ?title])]}
             (rdf/sparql->datalog "
PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
PREFIX dc11:  <http://purl.org/dc/elements/1.1/>

SELECT ?title
WHERE  { { ?book dc10:title  ?title } UNION { ?book dc11:title  ?title } }")))

    ;; TODO: this should really be working like optional and select
    ;; both ?x and ?y and not ?book
    (t/is (= '{:find [?book],
               :where
               [(or-join [?book]
                         [?book :http://purl.org/dc/elements/1.0/title ?x]
                         [?book :http://purl.org/dc/elements/1.1/title ?y])]}
             (rdf/sparql->datalog "
PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
PREFIX dc11:  <http://purl.org/dc/elements/1.1/>

SELECT ?book
WHERE  { { ?book dc10:title ?x } UNION { ?book dc11:title  ?y } }")))


    (t/is (= '{:find [?title ?author],
               :where
               [(or (and [?book :http://purl.org/dc/elements/1.0/title ?title]
                         [?book :http://purl.org/dc/elements/1.0/creator ?author])
                    (and [?book :http://purl.org/dc/elements/1.1/title ?title]
                         [?book :http://purl.org/dc/elements/1.1/creator ?author]))]}
             (rdf/sparql->datalog "
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
                   :http://xmlns.com/foaf/0.1/Person]
                  (not-join [?person] [?person :http://xmlns.com/foaf/0.1/name ?name])]})
             (rdf/sparql->datalog "
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
                   :http://xmlns.com/foaf/0.1/Person]
                  [?person :http://xmlns.com/foaf/0.1/name ?name]]})
             (rdf/sparql->datalog "
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
                            (rdf/sparql->datalog
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
               :where [(http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR ?x ?type)]
               :rules [[(http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR ?s ?o)
                        [?s :http://www.w3.org/2000/01/rdf-schema#subClassOf ?o]]
                       [(http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR ?s ?o)
                        [?s :http://www.w3.org/2000/01/rdf-schema#subClassOf ?t]
                        (http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR ?t ?o)]
                       [(http://www.w3.org/2000/01/rdf-schema#subClassOf-STAR ?s ?o)
                        [?s :crux.db/id]
                        [:crux.rdf/zero-matches ?o]]]}
             (rdf/sparql->datalog "
PREFIX  rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?x ?type
{
  ?x  rdfs:subClassOf* ?type
}")))

    (t/is (= '{:find [?person],
               :where [(http://xmlns.com/foaf/0.1/knows-PLUS :http://example/x ?person)]
               :rules [[(http://xmlns.com/foaf/0.1/knows-PLUS ?s ?o)
                        [?s :http://xmlns.com/foaf/0.1/knows ?o]]
                       [(http://xmlns.com/foaf/0.1/knows-PLUS ?s ?o)
                        [?s :http://xmlns.com/foaf/0.1/knows ?t]
                        (http://xmlns.com/foaf/0.1/knows-PLUS ?t ?o)]]}
             (rdf/sparql->datalog "
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
                         (and [:http://example/x :crux.db/id]
                              [:crux.rdf/zero-matches ?person])
                         [:http://example/x :http://xmlns.com/foaf/0.1/knows ?person])]}
             (rdf/sparql->datalog "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX :     <http://example/>
SELECT ?person
{
  :x foaf:knows? ?person
}")))

    (t/is (thrown-with-msg? UnsupportedOperationException #"Nested expressions are not supported."
                            (rdf/sparql->datalog "
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
               [[?book :http://purl.org/dc/elements/1.1/title ?title]
                [?book :http://example.org/ns#price ?price]],
               :args
               [{?book :http://example.org/book/book1}
                {?book :http://example.org/book/book3}]}
             (rdf/sparql->datalog "
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
               [[?book :http://purl.org/dc/elements/1.1/title ?title]
                [?book :http://example.org/ns#price ?price]],
               :args
               [{?book :crux.rdf/undefined, ?title "SPARQL Tutorial"}
                {?book :http://example.org/book/book2, ?title :crux.rdf/undefined}]}
             (rdf/sparql->datalog "
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
               [[?book :http://purl.org/dc/elements/1.1/title ?title]
                [?book :http://example.org/ns#price ?price]],
               :args
               [{?book :crux.rdf/undefined, ?title "SPARQL Tutorial"}
                {?book :http://example.org/book/book2, ?title :crux.rdf/undefined}]}
             (rdf/sparql->datalog "
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
               :where [[?x :http://xmlns.com/foaf/0.1/name ?name]]
               :limit 20
               :order-by [[?name :asc]]}
             (rdf/sparql->datalog
              "
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>

SELECT ?name
WHERE { ?x foaf:name ?name }
ORDER BY ?name
LIMIT 20
")))))
