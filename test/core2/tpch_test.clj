(ns core2.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.ingester :as ingest]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util]
            [core2.operator :as op])
  (:import [java.nio.file Files LinkOption Path]
           [java.time Clock Duration ZoneId]))

(def ^:dynamic *node* nil)
(def ^:dynamic *db* nil)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [^Path node-dir scale-factor clock]} f]
  (util/delete-dir node-dir)

  (with-open [node (tu/->local-node {:node-dir node-dir,
                                     :clock clock})]
    (let [last-tx (-> (tpch/submit-docs! node scale-factor)
                      (tu/then-await-tx node))]
      (tu/finish-chunk node)

      (let [db (ingest/snapshot (tu/component node :core2/ingester)
                                last-tx
                                (Duration/ofMinutes 1))]
        (binding [*node* node, *db* db]
          (f))))))

(defn- tpch-node-dir ^Path [scale-factor]
  (util/->path (format "target/can-submit-tpch-docs-%s" scale-factor)))

(defn- tpch-test-dir ^Path [scale-factor]
  (.toPath (io/as-file (io/resource (format "can-submit-tpch-docs-%s/" scale-factor)))))

(defn- object-files [^Path node-dir]
  (iterator-seq (.iterator (Files/list (.resolve node-dir "objects")))))

(defn- paired-paths
  "Seq of pairs of files [test-file, written-file] where test-file is a file in the test-resources/can-submit-tpch-docs-xxxx
  dir, and written-file is the matching file in the node-dir.

  Useful to compare or replace expected .json with the ingest output."
  [scale-factor ^Path node-dir]
  (let [objects-dir (.resolve node-dir "objects")
        test-dir (tpch-test-dir scale-factor)]
    (for [test-path (iterator-seq (.iterator (Files/list test-dir)))
          :let [written-object-path (.resolve objects-dir (.relativize test-dir test-path))]]
      [test-path written-object-path])))

(defn- test-tpch-ingest [scale-factor expected-objects]
  (let [node-dir (tpch-node-dir scale-factor)]
    (with-tpch-data
      {:node-dir node-dir
       :scale-factor scale-factor
       :clock (Clock/fixed (util/->instant #inst "2021-04-01") (ZoneId/of "UTC"))}
      (fn []
        (t/is (= expected-objects (count (object-files node-dir))))
        (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))
        (doseq [[expected-path actual-path] (paired-paths scale-factor node-dir)]
          (t/is (Files/exists actual-path (make-array LinkOption 0)))
          (tu/check-json-file expected-path actual-path))))))

(defn run-query
  ([q] (run-query q {}))
  ([q args]
   (op/query-ra q (merge {'$ *db*}
                         (::tpch/params (meta q))
                         args))))

(defn slurp-query [query-no]
  (slurp (io/resource (str "core2/sql/tpch/" (format "q%02d.sql" query-no)))))

(t/deftest ^:integration can-submit-tpch-docs-0.01
  (test-tpch-ingest 0.01 67))

(t/deftest can-submit-tpch-docs-0.001
  (test-tpch-ingest 0.001 67))
