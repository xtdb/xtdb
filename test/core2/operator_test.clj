(ns core2.operator-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.operator :as op]
            [core2.select :as sel]
            [core2.util :as util]
            [core2.test-util :as tu]
            [core2.metadata :as meta]
            [core2.types :as ty])
  (:import java.util.function.Consumer
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector.util Text]))

(t/deftest test-find-gt-ivan
  (let [node-dir (doto (util/->path "target/test-find-gt-ivan")
                   util/delete-dir)]
    (with-open [node (c2/->local-node node-dir {:max-rows-per-chunk 10, :max-rows-per-block 2})
                tx-producer (c2/->local-tx-producer node-dir)]
      (let [allocator (.allocator node)
            buffer-pool (.buffer-pool node)
            metadata-mgr (.metadata-manager node)
            temporal-mgr (.temporal-manager node)]

        @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Håkan", :_id 0}}])
             (tu/then-await-tx node))

        (tu/finish-chunk node)

        @(c2/submit-tx tx-producer [{:op :put, :doc {:name "James", :_id 1}}
                                 {:op :put, :doc {:name "Dan", :_id 2}}])

        @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Jon", :_id 3}}])
             (tu/then-await-tx node))

        (tu/finish-chunk node)

        (let [ivan-pred (sel/->str-pred sel/pred> "Ivan")
              metadata-pred (meta/matching-chunk-pred "name" ivan-pred Types$MinorType/VARCHAR)
              op-factory (op/->operator-factory allocator metadata-mgr temporal-mgr buffer-pool)]

          (letfn [(query-ivan [watermark]
                    (let [!results (atom [])]
                      (with-open [chunk-scanner (.scan op-factory watermark
                                                       ["name"] metadata-pred
                                                       {"name" (sel/->dense-union-pred ivan-pred (ty/arrow-type->type-id (.getType Types$MinorType/VARCHAR)))}
                                                       nil
                                                       nil)]
                        (while (.tryAdvance chunk-scanner
                                            (reify Consumer
                                              (accept [_ root]
                                                (swap! !results into (tu/root->rows root)))))))
                      (set @!results)))]
            (with-open [watermark (c2/open-watermark node)]
              (t/is (= [1] (meta/matching-chunks metadata-mgr watermark metadata-pred))
                    "only needs to scan chunk 1")

              @(-> (c2/submit-tx tx-producer [{:op :put, :doc {:name "Jeremy", :_id 4}}])
                   (tu/then-await-tx node))

              (t/is (= #{[(Text. "James")]
                         [(Text. "Jon")]}
                       (query-ivan watermark))))

            (with-open [watermark (c2/open-watermark node)]
              (t/is (= #{[(Text. "James")]
                         [(Text. "Jon")]
                         [(Text. "Jeremy")]}
                       (query-ivan watermark))))))))))
