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
  ;; http://docs.rdf4j.org/rdf-tutorial/#_databases_and_sparql_querying
  (t/testing "RDF4J Example"
    (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
               '{:find [?s ?n]
                 :where
                 [[?s :rdf/type :http://example.org/Artist]
                  [?s :foaf/firstName ?n]]})
             (crux.rdf/sparql->datalog
              "
PREFIX ex: <http://example.org/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?s ?n
WHERE {
   ?s a ex:Artist;
      foaf:firstName ?n .
}"))))

  ;; https://jena.apache.org/tutorials/sparql.html
  (t/testing "Apacha Jena Tutorial"
    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?x]
                 :where [[?x :vcard/FN "John Smith"]]})
             (crux.rdf/sparql->datalog
              "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }")))

    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?y ?givenName]
                 :where [[?y :vcard/Family "Smith"]
                         [?y :vcard/Given ?givenName]]})
             (crux.rdf/sparql->datalog
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
             (pr-str (crux.rdf/sparql->datalog
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
             (crux.rdf/sparql->datalog
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
                 :where [(or [_ :foaf/name ?name]
                             [_ :vcard/FN ?name])]})
             (crux.rdf/sparql->datalog
              "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX vCard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?name
WHERE
{
   { [] foaf:name ?name } UNION { [] vCard:FN ?name }
}"))))

  ;; https://www.w3.org/TR/2013/REC-sparql11-query-20130321

  (t/testing "SPARQL 1.1"
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

    (t/is (thrown-with-msg? UnsupportedOperationException #"OPTIONAL not supported."
                            (rdf/sparql->datalog
                             "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE  { ?x foaf:name  ?name .
         OPTIONAL { ?x  foaf:mbox  ?mbox }
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

    (t/is (thrown-with-msg? UnsupportedOperationException #"Nested mathematical expressions are not supported."
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
")))))
