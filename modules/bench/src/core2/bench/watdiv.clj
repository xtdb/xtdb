(ns core2.bench.watdiv
  (:require [clojure.java.io :as io]
            [core2.bench :as bench]
            [core2.api :as c2]
            [core2.local-node :as node])
  (:import java.util.concurrent.TimeUnit))

(comment
  ;; run in crux.bench.watdiv-crux.

  ;; had to temporarily replace a few calls to `keyword` within crux-rdf with just `str` too -
  ;; Core2 doesn't like URLs as keywords (well, Classic shouldn't either, but that's another story.)

  (with-open [in (io/input-stream "crux-bench/data/watdiv.10M.nt")
              out (io/writer (io/file "/tmp/watdiv-10M.edn"))]
    (doseq [doc (-> (rdf/ntriples-seq in)
                    rdf/statements->maps)]
      (.write out (prn-str doc))))

  (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
    (fn [queries]
      (with-open [out (io/writer "/tmp/test.1.edn")]
        (doseq [{query :q} queries]
          (.write out (prn-str (sparql/sparql->datalog query))))))))

(defn- ingest-watdiv [node file]
  (let [tx (bench/with-timing :submit-docs
             (with-open [rdr (io/reader file)]
               (doseq [doc-batch (->> (line-seq rdr)
                                      (map read-string)
                                      (partition-all 100))]
                 (c2/submit-tx node (for [doc doc-batch]
                                      ;; TODO Core2 doesn't support set vals yet
                                      [:put (->> doc (into {} (remove (comp set? val))))])))))]
    (bench/with-timing :await-tx
      @(-> (node/await-tx-async node tx)
           (.orTimeout 5 TimeUnit/HOURS)))

    (bench/with-timing :finish-chunk
      (bench/finish-chunk node))))

(defn- query-watdiv [node query-file]
  ;; TODO currently fails because it doesn't like strings as attributes

  (with-open [query-rdr (io/reader query-file)]
    (let [basis-tx (c2/latest-completed-tx node)]
      (doseq [[idx query] (->> (line-seq query-rdr)
                               (map read-string)
                               (map-indexed vector))]
        (bench/with-timing (keyword (str "query-" idx))
          (count (->> (c2/plan-query node (assoc query :basis {:tx basis-tx}))
                      (into []))))))))

(comment
  (with-open [node (node/start-node {})]
    (ingest-watdiv node (io/file "/tmp/watdiv-10M.edn"))
    (query-watdiv node (io/file "/tmp/watdiv-stress-100-queries.1.edn"))))

(defn -main []
  (let [docs-file (bench/with-timing :download-doc-file
                    (doto (bench/tmp-file-path "watdiv-10M." ".edn")
                      (->> (bench/download-s3-dataset-file "watdiv/watdiv-10M.edn"))))
        query-file (doto (bench/tmp-file-path "watdiv-stress-100-test.1." ".edn")
                     (->> (bench/download-s3-dataset-file "watdiv/watdiv-stress-100/test.1.edn")))]
    (with-open [node (bench/start-node)]
      (bench/with-timing :ingest
        (ingest-watdiv node docs-file))

      (bench/with-timing :queries
        (query-watdiv node query-file)))))
