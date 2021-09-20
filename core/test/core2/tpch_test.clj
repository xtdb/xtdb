(ns core2.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util]
            [core2.operator :as op]
            [core2.snapshot :as snap])
  (:import [java.nio.file Files LinkOption Path]
           [java.time Clock Duration ZoneId]))

(def ^:dynamic *node*)
(def ^:dynamic *db*)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [^Path node-dir scale-factor clock]} f]
  (util/delete-dir node-dir)

  (with-open [node (tu/->local-node {:node-dir node-dir,
                                     :clock clock})]
    (let [last-tx (-> (tpch/submit-docs! node scale-factor)
                      (tu/then-await-tx node))]
      (tu/finish-chunk node)

      (let [db (snap/snapshot (tu/component node ::snap/snapshot-factory)
                              last-tx
                              (Duration/ofMinutes 1))]
        (binding [*node* node, *db* db]
          (f))))))

(defn- test-tpch-ingest [scale-factor expected-objects]
  (let [node-dir (util/->path (format "target/can-submit-tpch-docs-%s" scale-factor))
        objects-dir (.resolve node-dir "objects")]
    (with-tpch-data {:node-dir node-dir
                     :scale-factor scale-factor
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
   (op/query-ra q (merge {'$ *db*}
                         (::tpch/params (meta q))
                         args))))

(t/deftest ^:integration can-submit-tpch-docs-0.01
  (test-tpch-ingest 0.01 67))

(t/deftest can-submit-tpch-docs-0.001
  (test-tpch-ingest 0.001 67))
