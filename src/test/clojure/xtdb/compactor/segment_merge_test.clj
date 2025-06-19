(ns xtdb.compactor.segment-merge-test
  (:require [clojure.test :as t]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           (xtdb.compactor SegmentMerge SegmentMerge$Result SegmentMerge$RecencyPartitioning$Partition SegmentMerge$RecencyPartitioning$Preserve)
           [xtdb.trie DataRel]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-merges-segments
  (with-open [seg-merge (SegmentMerge. tu/*allocator*)]
    (util/with-open [lt0 (tu/open-live-table "foo")
                     lt1 (tu/open-live-table "foo")]

      (tu/index-tx! lt0 #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 0}
                     {:xt/id "bar", :v 0}])

      (tu/index-tx! lt0 #xt/tx-key {:tx-id 1, :system-time #xt/instant "2021-01-01T00:00:00Z"}
                    [{:xt/id "bar", :v 1}])

      (tu/index-tx! lt1 #xt/tx-key {:tx-id 2, :system-time #xt/instant "2022-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 1}])

      (tu/index-tx! lt1 #xt/tx-key {:tx-id 3, :system-time #xt/instant "2023-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 2}
                     {:xt/id "bar", :v 2}])

      (with-open [live-rel0 (.openDirectSlice (.getLiveRelation lt0) tu/*allocator*)
                  live-rel1 (.openDirectSlice (.getLiveRelation lt1) tu/*allocator*)]

        (let [segments [(-> (trie/->Segment (.compactLogs (.getLiveTrie lt0)))
                            (assoc :data-rel (DataRel/live live-rel0)))
                        (-> (trie/->Segment (.compactLogs (.getLiveTrie lt1)))
                            (assoc :data-rel (DataRel/live live-rel1)))]]

          (t/testing "merge segments"
            (util/with-open [results (.mergeSegments seg-merge segments nil (SegmentMerge$RecencyPartitioning$Preserve. nil))]
              (t/is (= [[{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from #xt/zdt "2023-01-01Z[UTC]"
                          :xt/valid-from #xt/zdt "2023-01-01Z[UTC]"
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 2, :xt/id "bar"}}
                         {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from (time/->zdt #inst "2021")
                          :xt/valid-from (time/->zdt #inst "2021")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 1, :xt/id "bar"}}
                         {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from (time/->zdt #inst "2020")
                          :xt/valid-from (time/->zdt #inst "2020")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 0, :xt/id "bar"}}
                         {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                          :xt/system-from (time/->zdt #inst "2023")
                          :xt/valid-from (time/->zdt #inst "2023")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 2, :xt/id "foo"}}
                         {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                          :xt/system-from (time/->zdt #inst "2022")
                          :xt/valid-from (time/->zdt #inst "2022")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 1, :xt/id "foo"}}
                         {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                          :xt/system-from (time/->zdt #inst "2020")
                          :xt/valid-from (time/->zdt #inst "2020")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 0, :xt/id "foo"}}]]

                       (for [^SegmentMerge$Result res results]
                         (with-open [rel (.openAllAsRelation seg-merge res)]
                           (->> (.toMaps rel)
                                (mapv #(update % :xt/iid (comp util/byte-buffer->uuid ByteBuffer/wrap))))))))))

          (t/testing "merge segments with path predicate"
            (util/with-open [results (.mergeSegments seg-merge segments (byte-array [2]) (SegmentMerge$RecencyPartitioning$Preserve. nil))]
              (t/is (= [[{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from (time/->zdt #inst "2023")
                          :xt/valid-from (time/->zdt #inst "2023")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 2, :xt/id "bar"}}
                         {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from (time/->zdt #inst "2021")
                          :xt/valid-from (time/->zdt #inst "2021")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 1, :xt/id "bar"}}
                         {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                          :xt/system-from (time/->zdt #inst "2020")
                          :xt/valid-from (time/->zdt #inst "2020")
                          :xt/valid-to (time/->zdt time/end-of-time)
                          :op {:v 0, :xt/id "bar"}}]]

                       (for [^SegmentMerge$Result res results]
                         (with-open [rel (.openAllAsRelation seg-merge res)]
                           (->> (.toMaps rel)
                                (mapv #(update % :xt/iid (comp util/byte-buffer->uuid ByteBuffer/wrap))))))))))

          (t/testing "merge segments partitioning by recency"
            (util/with-open [results (.mergeSegments seg-merge segments nil SegmentMerge$RecencyPartitioning$Partition/INSTANCE)]
              (t/is (= {"r20210104.arrow" [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                                            :xt/system-from (time/->zdt #inst "2020")
                                            :xt/valid-from (time/->zdt #inst "2020")
                                            :xt/valid-to (time/->zdt time/end-of-time)
                                            :op {:v 0, :xt/id "bar"}}]

                        "r20220103.arrow" [{:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                                            :xt/system-from (time/->zdt #inst "2020")
                                            :xt/valid-from (time/->zdt #inst "2020")
                                            :xt/valid-to (time/->zdt time/end-of-time)
                                            :op {:v 0, :xt/id "foo"}}]

                        "r20230102.arrow" [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                                            :xt/system-from (time/->zdt #inst "2021")
                                            :xt/valid-from (time/->zdt #inst "2021")
                                            :xt/valid-to (time/->zdt time/end-of-time)
                                            :op {:v 1, :xt/id "bar"}}
                                           {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                                            :xt/system-from #xt/zoned-date-time "2022-01-01T00:00Z[UTC]",
                                            :xt/valid-from #xt/zoned-date-time "2022-01-01T00:00Z[UTC]",
                                            :xt/valid-to (time/->zdt time/end-of-time)
                                            :op {:xt/id "foo", :v 1}}]

                        "rc.arrow" [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                                     :xt/system-from (time/->zdt #inst "2023")
                                     :xt/valid-from (time/->zdt #inst "2023")
                                     :xt/valid-to (time/->zdt time/end-of-time)
                                     :op {:v 2, :xt/id "bar"}}
                                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                                     :xt/system-from (time/->zdt #inst "2023")
                                     :xt/valid-from (time/->zdt #inst "2023")
                                     :xt/valid-to (time/->zdt time/end-of-time)
                                     :op {:v 2, :xt/id "foo"}}]}

                       (->> (for [^SegmentMerge$Result res results]
                              [(str (.getFileName (.getPath$xtdb_core res)))
                               (with-open [rel (.openAllAsRelation seg-merge res)]
                                 (->> (.toMaps rel)
                                      (mapv #(update % :xt/iid (comp util/byte-buffer->uuid ByteBuffer/wrap)))))])
                            (into {})))))))))))
