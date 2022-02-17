(ns xtdb.bench.watdiv
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [xtdb.rdf :as rdf]
            [xtdb.sparql :as sparql])
  (:import (org.eclipse.rdf4j.model IRI)))

(def watdiv-tests
  {"watdiv-stress-100/warmup.1.desc" "watdiv-stress-100/warmup.sparql"
   "watdiv-stress-100/test.1.desc" "watdiv-stress-100/test.1.sparql"
   "watdiv-stress-100/test.2.desc" "watdiv-stress-100/test.2.sparql"
   "watdiv-stress-100/test.3.desc" "watdiv-stress-100/test.3.sparql"
   "watdiv-stress-100/test.4.desc" "watdiv-stress-100/test.4.sparql"})

(def query-timeout-ms 30000)

(def watdiv-input-file (io/resource "watdiv.10M.nt"))
(def watdiv-stress-100-1-sparql (io/resource "watdiv-stress-100/test.1.sparql"))

(defn with-watdiv-queries [resource f]
  (with-open [sparql-in (io/reader resource)]
    (f (->> (line-seq sparql-in)
            (map-indexed (fn [idx q]
                           {:idx idx, :q q}))))))

(def namespace-map
  {"http://db.uwaterloo.ca/~galuc/wsdbm/" "watdiv"
   "http://ogp.me/ns#" "me.ogp"
   "http://purl.org/dc/terms/" "purl.dc.terms"
   "http://purl.org/goodrelations/" "purl.goodrelations"
   "http://purl.org/ontology/mo/" "purl.ontology.mo"
   "http://purl.org/stuff/rev#" "purl.stuff.rev"
   "http://schema.org/" "org.schema"
   "http://www.geonames.org/ontology#" "geonames.ontology"
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#" "org.w3.rdf"
   "http://xmlns.com/foaf/" "foaf"})

(defn iri->kw [^IRI iri]
  (keyword (let [ns (.getNamespace iri)]
             (get namespace-map ns ns))
           (.getLocalName iri)))

(comment
  (with-redefs [rdf/iri->kw iri->kw]
    (with-watdiv-queries watdiv-stress-100-1-sparql
      (fn [qs]
        (with-open [w (io/writer (io/resource "xtdb/fixtures/watdiv/100-queries.edn"))]
          (doseq [{:keys [q]} (take 100 qs)]
            (->> (sparql/sparql->datalog q)
                 prn-str
                 (.write w))))))))

(defn submit-watdiv! [node]
  (with-redefs [rdf/iri->kw iri->kw]
    (with-open [in (io/input-stream watdiv-input-file)]
      (rdf/submit-ntriples node in 1000))))

(comment
  (submit-watdiv! (dev/xtdb-node)))
