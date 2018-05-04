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
   (let [artist (keyword "http://example.org" "Picasso")
         painting (-> iri->entity
                      (artist)
                      ((keyword "http://example.org" "creatorOf")))
         address (-> iri->entity
                     (artist)
                     ((keyword "http://example.org" "homeAddress")))]
     (t/is (= (keyword "http://example.org" "guernica") painting))
     (t/is (= "oil on canvas"
              (-> iri->entity
                  (painting)
                  ((keyword "http://example.org" "technique")))))
     (t/is (= {(keyword "http://example.org" "street") "31 Art Gallery",
               (keyword "http://example.org" "city") "Madrid",
               (keyword "http://example.org" "country") "Spain"}
              (-> iri->entity
                  (address)
                  (dissoc :crux.kv/id)))))))
