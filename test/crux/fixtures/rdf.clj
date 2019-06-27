(ns crux.fixtures.rdf
  (:require [crux.rdf :as rdf]
            [clojure.java.io :as io]))

(defn ntriples [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (doall
     (->> (rdf/ntriples-seq in)
          (rdf/statements->maps)))))

(defn ->tx-ops [ntriples]
  (vec (for [entity ntriples]
         [:crux.tx/put entity])))

(defn ->default-language [c]
  (mapv #(rdf/use-default-language % :en) c))

(defn ->maps-by-id [rdf-maps]
  (->> (for [m rdf-maps]
         {(:crux.db/id m) m})
       (into {})))
