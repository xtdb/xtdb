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
(t/deftest test-can-parse-ntriples-into-maps
  (let [iri->entity (load-example "crux/example-data-artists.nt")]
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
                   (dissoc :crux.kv/id)))))))
