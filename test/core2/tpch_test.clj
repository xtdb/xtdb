(ns core2.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util])
  (:import [java.nio.file Files LinkOption]
           [java.time Clock Duration ZoneId]))

(defn- test-tpch-ingest [scale-factor expected-objects]
  (let [node-dir (util/->path (format "target/can-submit-tpch-docs-%s" scale-factor))
        objects-dir (.resolve node-dir "objects")
        mock-clock (Clock/fixed (.toInstant #inst "2021-04-01") (ZoneId/of "UTC"))]
    (util/delete-dir node-dir)

    (time
     (with-open [node (tu/->local-node {:node-dir node-dir})
                 tx-producer (tu/->local-tx-producer {:node-dir node-dir, :clock mock-clock})]
       (let [last-tx (tpch/submit-docs! tx-producer scale-factor)]
         (c2/await-tx node last-tx (Duration/ofMinutes 1))

         (tu/finish-chunk node)
         (t/is (= expected-objects
                  (count (iterator-seq (.iterator (Files/list objects-dir)))))))))

    (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))

    (let [expected-dir (.toPath (io/as-file (io/resource (format "can-submit-tpch-docs-%s/" scale-factor))))]
      (doseq [expected-path (iterator-seq (.iterator (Files/list expected-dir)))
              :let [actual-path (.resolve objects-dir (.relativize expected-dir expected-path))]]
        (t/is (Files/exists actual-path (make-array LinkOption 0)))
        (tu/check-json-file expected-path actual-path)))))

(t/deftest ^:integration can-submit-tpch-docs-0.01
  (test-tpch-ingest 0.01 225))

(t/deftest can-submit-tpch-docs-0.001
  (test-tpch-ingest 0.001 67))
