(ns core2.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util])
  (:import [java.nio.file Files LinkOption Path]
           [java.time Clock Duration ZoneId]))

(def ^:dynamic *node*)
(def ^:dynamic *db*)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [^Path node-dir scale-factor clock compress-snapshots?]
                       :or {compress-snapshots? true}} f]
  (util/delete-dir node-dir)

  (with-open [node (tu/->local-node {:node-dir node-dir,
                                     :clock clock
                                     :compress-snapshots? compress-snapshots?})]
    (let [last-tx (tpch/submit-docs! node scale-factor)]
      (c2/await-tx node last-tx (Duration/ofMinutes 1))

      (tu/finish-chunk node))

    (with-open [db (c2/open-db node)]
      (binding [*node* node
                *db* db]
        (f)))))

(defn- test-tpch-ingest [scale-factor expected-objects]
  (let [node-dir (util/->path (format "target/can-submit-tpch-docs-%s" scale-factor))
        objects-dir (.resolve node-dir "objects")]
    (with-tpch-data {:node-dir node-dir
                     :scale-factor scale-factor
                     :compress-snapshots? false
                     :clock (Clock/fixed (.toInstant #inst "2021-04-01") (ZoneId/of "UTC"))}
      (fn []
        (t/is (= expected-objects
                 (count (iterator-seq (.iterator (Files/list objects-dir))))))
        (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))

        (let [expected-dir (.toPath (io/as-file (io/resource (format "can-submit-tpch-docs-%s/" scale-factor))))]
          (doseq [expected-path (iterator-seq (.iterator (Files/list expected-dir)))
                  :let [actual-path (.resolve objects-dir (.relativize expected-dir expected-path))]]
            (t/is (Files/exists actual-path (make-array LinkOption 0)))
            (tu/check-json-file expected-path actual-path)))))))

(defn run-query
  ([q] (run-query q {}))
  ([q args]
   (with-open [res (c2/open-q (merge {'$ *db*
                                      'q16-psizes tpch/tpch-q16-psizes
                                      'q22-cntrycodes tpch/tpch-q22-cntrycodes}
                                     args)
                              q)]
     (into [] (mapcat seq) (tu/<-cursor res)))))

(t/deftest ^:integration can-submit-tpch-docs-0.01
  (test-tpch-ingest 0.01 68))

(t/deftest can-submit-tpch-docs-0.001
  (test-tpch-ingest 0.001 68))
