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
                 [[?s :rdf/type
                   :http://example.org/Artist]
                  [?s :foaf/firstName ?n]]})
             (crux.rdf/parse-sparql
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
             (crux.rdf/parse-sparql
              "
SELECT ?x
WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }")))

    (t/is (= (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
               '{:find [?y ?givenName]
                 :where [[?y :vcard/Family "Smith"]
                         [?y :vcard/Given ?givenName]]})
             (crux.rdf/parse-sparql
              "
SELECT ?y ?givenName
WHERE
  { ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Family>  \"Smith\" .
    ?y  <http://www.w3.org/2001/vcard-rdf/3.0#Given>  ?givenName .
  }")))

    (t/is (= (pr-str (rdf/with-prefix {:vcard "http://www.w3.org/2001/vcard-rdf/3.0#"}
                       '{:find [?g]
                         :where [[(re-find #"(?i)r" ?g)]
                                 [?y :vcard/Given ?g]]}))
             (pr-str (crux.rdf/parse-sparql
                      "
PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>

SELECT ?g
WHERE
{ ?y vcard:Given ?g .
  FILTER regex(?g, \"r\", \"i\") }"))))

    (t/is (= (rdf/with-prefix {:info "http://somewhere/peopleInfo#"}
               '{:find [?resource]
                 :where [[(>= ?age 24)]
                         [?resource :info/age ?age]]})
             (crux.rdf/parse-sparql
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
             (crux.rdf/parse-sparql
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
    (t/is (thrown? UnsupportedOperationException
                   (rdf/parse-sparql
                    "SELECT ?v WHERE { ?v ?p \"cat\"@en }")))

    (t/is
     (= '{:find [?v],
          :where
          [[?v :http://xmlns.com/foaf/0.1/givenName "cat"]]}
        (rdf/parse-sparql
         "SELECT ?v WHERE { ?v <http://xmlns.com/foaf/0.1/givenName> \"cat\"@en }")))

    (t/is
     (= '{:find [?name],
          :where
          [[(http://www.w3.org/2005/xpath-functions#concat ?G " " ?S) ?name]
           [?P :http://xmlns.com/foaf/0.1/givenName ?G]
           [?P :http://xmlns.com/foaf/0.1/surname ?S]]}
        (rdf/parse-sparql
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
          [[(http://www.w3.org/2005/xpath-functions#concat ?G " " ?S) ?name]
           [?P :http://xmlns.com/foaf/0.1/givenName ?G]
           [?P :http://xmlns.com/foaf/0.1/surname ?S]]}
        (rdf/parse-sparql
         "
PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
SELECT ( CONCAT(?G, \" \", ?S) AS ?name )
WHERE  { ?P foaf:givenName ?G ; foaf:surname ?S }
")))

    (t/is (= (pr-str '{:find [?title],
                       :where
                       [[(re-find #"^SPARQL" ?title)]
                        [?x :http://purl.org/dc/elements/1.1/title ?title]]})
             (pr-str (rdf/parse-sparql
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"^SPARQL\")
        }"))))

    (t/is (= (pr-str '{:find [?title],
                       :where
                       [[(re-find #"(?i)web" ?title)]
                        [?x :http://purl.org/dc/elements/1.1/title ?title]]})
             (pr-str (rdf/parse-sparql
                      "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
SELECT  ?title
WHERE   { ?x dc:title ?title
          FILTER regex(?title, \"web\", \"i\" )
        }"))))

    (t/is (= '{:find [?title ?price],
               :where
               [[(< ?price 30.5M)]
                [?x :http://example.org/ns#price ?price]
                [?x :http://purl.org/dc/elements/1.1/title ?title]]}
             (rdf/parse-sparql
              "
PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
PREFIX  ns:  <http://example.org/ns#>
SELECT  ?title ?price
WHERE   { ?x ns:price ?price .
          FILTER (?price < 30.5)
          ?x dc:title ?title . }")))

    (t/is (thrown? UnsupportedOperationException
                   (rdf/parse-sparql
                    "
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox
WHERE  { ?x foaf:name  ?name .
         OPTIONAL { ?x  foaf:mbox  ?mbox }
       }")))))
