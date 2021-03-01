(ns core2.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.select :as sel]
            [core2.scan :as scan]
            [core2.util :as util]
            [core2.test-util :as tu]
            [core2.metadata :as meta]
            [core2.types :as ty])
  (:import clojure.lang.MapEntry
           core2.core.IngestLoop
           core2.indexer.Indexer
           core2.scan.ScanFactory
           java.util.function.Consumer
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.util Text]))

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

        (let [ivan-pred (sel/->str-pred sel/pred> "Ivan")
              metadata-pred (meta/matching-chunk-pred "name" ivan-pred Types$MinorType/VARCHAR)]
          (t/is (= [1] (meta/matching-chunks metadata-mgr metadata-pred))
                "only needs to scan chunk 1")

          (let [scan-factory (ScanFactory. allocator metadata-mgr buffer-pool)]

            (letfn [(query-ivan [watermark]
                      (let [!results (atom [])]
                        (with-open [chunk-scanner (.scanBlocks scan-factory ["name"] metadata-pred
                                                               {"name" (sel/->dense-union-pred ivan-pred (ty/arrow-type->type-id (.getType Types$MinorType/VARCHAR)))})]
                          (while (.tryAdvance chunk-scanner
                                              (reify Consumer
                                                (accept [_ root]
                                                  (swap! !results into (tu/root->rows root)))))))
                        @!results))]
              (with-open [watermark (.getWatermark i)]
                @(-> (.submitTx tx-producer [{:op :put, :doc {:name "Jeremy", :id 4}}])
                     (tu/then-await-tx il))

                (t/is (= [[1 (Text. "James")]
                          [3 (Text. "Jon")]]
                         (query-ivan watermark))))

              (with-open [watermark (.getWatermark i)]
                (t/is (= [[1 (Text. "James")]
                          [3 (Text. "Jon")]
                          [4 (Text. "Jeremy")]]
                         (query-ivan watermark)))))))))))
