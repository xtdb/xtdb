(ns xtdb.bench.watdiv
  (:require [clojure.java.io :as io]))

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
