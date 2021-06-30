(ns crux.rdf-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]))

;; Example based on:
;; https://github.com/eclipse/rdf4j-doc/blob/master/examples/src/main/resources/example-data-artists.ttl
(t/deftest test-can-parse-ntriples-into-maps
  (let [iri->entity (->> (rdf/ntriples "crux/example-data-artists.nt")
                         (rdf/->maps-by-id))]
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
  (let [picasso (-> (->> (rdf/ntriples "crux/Pablo_Picasso.ntriples")
                         (rdf/->maps-by-id))
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
