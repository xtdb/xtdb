(ns crux.rdf-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]))

(defn load-example [resource]
  (with-open [in (io/input-stream
                  (io/resource resource))]
    (->> (for [m (->> (rdf/ntriples-seq in)
                      (rdf/statements->maps))]
           {(:crux.kv/id m) m})
         (into {}))))

;; Example based on:
;; https://github.com/eclipse/rdf4j-doc/blob/master/examples/src/main/resources/example-data-artists.ttl
(t/deftest test-can-parse-n-triples-into-maps
 (let [iri->entity (load-example "crux/example-data-artists.nt")]
   (t/is (= 7 (count iri->entity)))
   (let [painting (-> iri->entity
                      #crux/iri :http://example.org/Picasso
                      #crux/iri :http://example.org/creatorOf)
         address (-> iri->entity
                     #crux/iri :http://example.org/Picasso
                     #crux/iri :http://example.org/homeAddress)]
     (t/is (= #crux/iri :http://example.org/guernica
              painting))
     (t/is (= "oil on canvas"
              (-> iri->entity
                  painting
                  #crux/iri :http://example.org/technique)))
     (t/is (= #crux/iri
              {:http://example.org/street "31 Art Gallery",
               :http://example.org/city "Madrid",
               :http://example.org/country "Spain"}
              (-> iri->entity
                  address
                  (dissoc :crux.kv/id)))))))
