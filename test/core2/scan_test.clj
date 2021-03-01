(ns core2.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.select :as sel]
            [core2.scan :as scan]
            [core2.util :as util]
            [core2.test-util :as tu])
  (:import clojure.lang.MapEntry
           core2.core.IngestLoop
           core2.indexer.Indexer
           core2.scan.ScanFactory
           java.util.function.Consumer
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector VarCharVector VectorSchemaRoot]))

(t/deftest test-find-gt-ivan
  (let [node-dir (doto (util/->path "target/test-find-gt-ivan")
                   util/delete-dir)]
    (with-open [node (c2/->local-node node-dir {:max-rows-per-chunk 10, :max-rows-per-block 2})
                tx-producer (c2/->local-tx-producer node-dir)]
      (let [allocator (.allocator node)
            buffer-pool (.buffer-pool node)
            metadata-mgr (.metadata-manager node)
            ^Indexer i (.indexer node)
            ^IngestLoop il (.ingest-loop node)]

        @(-> (.submitTx tx-producer [{:op :put, :doc {:name "Håkan", :id 0}}])
             (tu/then-await-tx il))

        (.finishChunk i)

        @(.submitTx tx-producer [{:op :put, :doc {:name "James", :id 1}}
                                 {:op :put, :doc {:name "Dan", :id 2}}])

        @(-> (.submitTx tx-producer [{:op :put, :doc {:name "Jon", :id 3}}])
             (tu/then-await-tx il))

        (.finishChunk i)

        (let [scan-factory (ScanFactory. allocator metadata-mgr buffer-pool)
              !results (atom [])]
          (with-open [chunk-scanner (.scanBlocks scan-factory ["name"])]
            (while (.tryAdvance chunk-scanner
                                (reify Consumer
                                  (accept [_ root]
                                    (swap! !results into (tu/root->rows root)))))))
          (t/is (= [] @!results)))

        #_
        (let [ivan-pred (sel/->str-pred sel/pred> "Ivan")]
          (t/is (= [1] (c2/matching-chunks node "name" ivan-pred Types$MinorType/VARCHAR))
                "only needs to scan chunk 1")

          (letfn [(query-ivan [watermark]
                    (->> @(c2/scan node watermark "name" ivan-pred Types$MinorType/VARCHAR
                                   (fn [row-id ^VarCharVector field-vec idx]
                                     (MapEntry/create row-id (str (.getObject field-vec ^int idx)))))
                         (into {} (mapcat seq))))]

            (with-open [watermark (.getWatermark i)]
              @(-> (.submitTx tx-producer [{:op :put, :doc {:name "Jeremy", :id 4}}])
                   (tu/then-await-tx il))

              (t/is (= {1 "James", 3 "Jon"}
                       (query-ivan watermark))))

            (with-open [watermark (.getWatermark i)]
              (t/is (= {1 "James", 3 "Jon", 4 "Jeremy"}
                       (query-ivan watermark))))))))))
