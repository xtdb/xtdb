(ns xtdb.rdf-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [xtdb.rdf :as rdf]))

;; Example based on:
;; https://github.com/eclipse/rdf4j-doc/blob/master/examples/src/main/resources/example-data-artists.ttl
(t/deftest test-can-parse-ntriples-into-maps
  (let [iri->entity (->> (rdf/ntriples "xtdb/example-data-artists.nt")
                         (rdf/->maps-by-id))]
    (t/is (= 7 (count iri->entity)))

    (let [artist ((keyword "http://example.org/Picasso") iri->entity)
          painting ((keyword "http://example.org/creatorOf") artist)]

      (t/is (= (keyword "http://example.org/guernica") painting))
      (t/is (= "oil on canvas"
               (-> painting
                   iri->entity
                   (get (keyword "http://example.org/technique")))))

      (t/is (= {(keyword "http://example.org/street") "31 Art Gallery",
                (keyword "http://example.org/city") "Madrid",
                (keyword "http://example.org/country") "Spain"}
               (-> artist
                   (get (keyword "http://example.org/homeAddress"))
                   iri->entity
                   (dissoc :xt/id)))))))

(t/deftest test-can-parse-dbpedia-entity
  (let [picasso (-> (->> (rdf/ntriples "xtdb/Pablo_Picasso.ntriples")
                         (rdf/->maps-by-id))
                    (get (keyword "http://dbpedia.org/resource/Pablo_Picasso")))]
    (t/is (= 48 (count picasso)))
    (t/is (= {(keyword "http://xmlns.com/foaf/0.1/givenName") #xtdb.rdf.Lang{:en "Pablo"}
              (keyword "http://xmlns.com/foaf/0.1/surname") #xtdb.rdf.Lang{:en "Picasso"}
              (keyword "http://dbpedia.org/ontology/birthDate") #inst "1881-10-25"}
             (select-keys picasso
                          [(keyword "http://xmlns.com/foaf/0.1/givenName")
                           (keyword "http://xmlns.com/foaf/0.1/surname")
                           (keyword "http://dbpedia.org/ontology/birthDate")])))

    (t/is (= {(keyword "http://xmlns.com/foaf/0.1/givenName") "Pablo"
              (keyword "http://xmlns.com/foaf/0.1/surname") "Picasso"}
             (select-keys (rdf/use-default-language picasso :en)
                          [(keyword "http://xmlns.com/foaf/0.1/givenName")
                           (keyword "http://xmlns.com/foaf/0.1/surname")])))))
