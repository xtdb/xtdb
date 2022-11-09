(ns core2.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util]
            [core2.node :as node])
  (:import [java.nio.file Files LinkOption Path]))

(def ^:dynamic *node* nil)
(def ^:dynamic *db* nil)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [method ^Path node-dir scale-factor]} f]
  (util/delete-dir node-dir)

  (with-open [node (tu/->local-node {:node-dir node-dir})]
    (let [last-tx (case method
                    :docs (tpch/submit-docs! node scale-factor)
                    :dml (tpch/submit-dml! node scale-factor))]
      (tu/then-await-tx last-tx node)
      (tu/finish-chunk node)

      (let [db @(node/snapshot-async node last-tx)]
        (binding [*node* node, *db* db]
          (f))))))

(def ^:private ^Path tpch-node-dir
  (util/->path (format "target/can-submit-tpch")))

(def ^:private ^Path tpch-test-dir
  (.toPath (io/as-file (io/resource "can-submit-tpch/"))))

(defn- object-files [^Path node-dir]
  (iterator-seq (.iterator (Files/list (.resolve node-dir "objects")))))

(defn- paired-paths
  "Seq of pairs of files [test-file, written-file] where test-file is a file in the test-resources/can-submit-tpch-docs-xxxx
  dir, and written-file is the matching file in the node-dir.

  Useful to compare or replace expected .json with the ingest output."
  [^Path node-dir, ^Path test-dir]
  (let [objects-dir (-> node-dir (.resolve "objects"))]
    (for [test-path (iterator-seq (.iterator (Files/list test-dir)))
          :let [written-object-path (.resolve objects-dir (.relativize test-dir test-path))]]
      [test-path written-object-path])))

(defn- test-tpch-ingest [method scale-factor expected-objects]
  (let [sub-dir (format "%s-%s" (name method) scale-factor)
        node-dir (.resolve tpch-node-dir sub-dir)
        test-dir (.resolve tpch-test-dir sub-dir)]
    (with-tpch-data {:method method
                     :node-dir node-dir
                     :scale-factor scale-factor}
      (fn []
        (t/is (= expected-objects (count (object-files node-dir))))
        (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))
        (doseq [[expected-path actual-path] (paired-paths node-dir test-dir)]
          (t/is (Files/exists actual-path (make-array LinkOption 0)))
          (tu/check-json-file expected-path actual-path))))))

(t/deftest ^:integration can-submit-tpch-docs-0.01
  (test-tpch-ingest :docs 0.01 67))

(t/deftest can-submit-tpch-docs-0.001
  (test-tpch-ingest :docs 0.001 67))

#_
(t/deftest ^:integration can-submit-tpch-dml-0.01
  (test-tpch-ingest :dml 0.01 66))

(t/deftest can-submit-tpch-dml-0.001
  (test-tpch-ingest :dml 0.001 67))

(defn run-query
  ([q] (run-query q {}))
  ([q args]
   (tu/query-ra q (merge {:srcs {'$ *db*}
                          :params (::tpch/params (meta q))}
                         args))))

(defn slurp-query [query-no]
  (slurp (io/resource (str "core2/sql/tpch/" (format "q%02d.sql" query-no)))))
