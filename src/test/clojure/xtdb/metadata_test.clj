(ns xtdb.metadata-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.metadata :as meta]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (clojure.lang MapEntry)
           (java.util.function IntPredicate)
           [org.apache.arrow.vector VectorSchemaRoot]
           (xtdb.buffer_pool IBufferPool)
           (xtdb.metadata IMetadataManager IMetadataPredicate)))

(t/use-fixtures :each tu/with-node)
(t/use-fixtures :once tu/with-allocator)

(t/deftest test-param-metadata-error-310
  (let [tx1 (xt/submit-tx tu/*node*
                          [[:sql-batch ["INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)"
                                        ["dave", "Dave", #inst "2018"]
                                        ["claire", "Claire", #inst "2019"]]]])]

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* ["SELECT users.name FROM users WHERE users.xt$id = ?" "dave"]
                   {:basis {:tx tx1}}))
          "#310")))

(deftest test-bloom-filter-for-num-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:num 0 :xt/id "a"}]
                                        [:put :xt_docs {:num 1 :xt/id "b"}]
                                        [:put :xt_docs {:num 1.0 :xt/id "c"}]
                                        [:put :xt_docs {:num 4 :xt/id "d"}]
                                        [:put :xt_docs {:num (short 3) :xt/id "e"}]
                                        [:put :xt_docs {:num 2.0 :xt/id "f"}]])
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:num 1} {:num 1.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 1)}]]
                          {:node tu/*node* :basis {:tx tx}})))

    (t/is (= [{:num 2.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 2)}]]
                          {:node tu/*node* :basis {:tx tx}})))

    (t/is (= [{:num 4}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:tx tx} :params {'?x (byte 4)}})))

    (t/is (= [{:num 3}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:tx tx} :params {'?x (float 3)}})))))

(deftest test-bloom-filter-for-datetime-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:timestamp #time/date "2010-01-01" :xt/id "a"}]
                                        [:put :xt_docs {:timestamp #time/zoned-date-time "2010-01-01T00:00:00Z" :xt/id "b"}]
                                        [:put :xt_docs {:timestamp #time/date-time "2010-01-01T00:00:00" :xt/id "c"}]
                                        [:put :xt_docs {:timestamp #time/date "2020-01-01" :xt/id "d"}]]
                             {:default-tz #time/zone "Z"})
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #time/zoned-date-time "2010-01-01T00:00:00Z")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp ?x)}]]
                          {:node tu/*node* :basis {:tx tx}
                           :default-tz  #time/zone "Z" :params {'?x #time/date "2010-01-01"}})))

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #time/date-time "2010-01-01T00:00:00")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))))

(deftest test-bloom-filter-for-time-types
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:time #time/time "01:02:03" :xt/id "a"}]
                                        [:put :xt_docs {:time #time/time "04:05:06" :xt/id "b"}]]
                             {:default-tz #time/zone "Z"})
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:time #time/time "04:05:06"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{time (= time #time/time "04:05:06")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))))

(deftest test-min-max-on-xt-id
  (with-open [node (node/start-node {:xtdb.indexer/live-index {:page-limit 16}})]
    (-> (xt/submit-tx node (for [i (range 20)] [:put :xt_docs {:xt/id i}]))
        (tu/then-await-tx node))

    (tu/finish-chunk! node)

    (let [^IBufferPool buffer-pool (tu/component node ::bp/buffer-pool)
          first-buckets (map (comp first tu/byte-buffer->path trie/->iid) (range 20))
          bucket->page-idx (->> (into (sorted-set) first-buckets)
                                (map-indexed #(MapEntry/create %2 %1))
                                (into {}))
          min-max-by-bucket (-> (group-by :bucket (map-indexed (fn [index bucket] {:index index :bucket bucket}) first-buckets))
                                (update-vals #(reduce (fn [res {:keys [index]}]
                                                        (-> res
                                                            (update :min min index)
                                                            (update :max max index)))
                                                      {:min Long/MAX_VALUE :max Long/MIN_VALUE}
                                                      %)))

          relevant-pages (->> (filter (fn [[_ {:keys [min max]}]] (<= min 10 max)) min-max-by-bucket)
                              (map (comp bucket->page-idx first)))

          ^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)
          literal-selector (expr.meta/->metadata-selector '(and (< xt/id 11) (> xt/id 9)) '{xt/id :i64} {})
          trie-obj-key (trie/->table-trie-obj-key "xt_docs" (trie/->trie-key 0 0 21))]
      (util/with-open [arrow-trie (trie/open-arrow-trie-file buffer-pool {:trie-file trie-obj-key})]
        (let [table-metadata (.tableMetadata metadata-mgr (:trie-rdr arrow-trie) trie-obj-key)
              page-idx-pred (.build literal-selector table-metadata)]

          (t/is (= #{"xt$iid" "xt$valid_to" "xt$valid_from" "xt$id" "xt$system_from"}
                   (.columnNames table-metadata)))

          (doseq [page-idx relevant-pages]
            (t/is (true? (.test page-idx-pred page-idx)))))))))

(deftest test-boolean-metadata
  (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id 1 :boolean-or-int true}]])
  (tu/finish-chunk! tu/*node*)

  (let [^IMetadataManager metadata-mgr (tu/component tu/*node* ::meta/metadata-manager)
        ^IBufferPool buffer-pool (tu/component tu/*node* ::bp/buffer-pool)
        true-selector (expr.meta/->metadata-selector '(= boolean-or-int true) '{boolean-or-int :bool} {})
        trie-obj-key (trie/->table-trie-obj-key "xt_docs" (trie/->trie-key 0 0 2))]

    (util/with-open [arrow-trie (trie/open-arrow-trie-file buffer-pool {:trie-file trie-obj-key})]
      (let [table-metadata (.tableMetadata metadata-mgr (:trie-rdr arrow-trie) trie-obj-key)
            page-idx-pred (.build true-selector table-metadata)]
        (t/is (= #{"xt$iid" "xt$valid_to" "xt$valid_from" "xt$id" "xt$system_from" "boolean_or_int"}
                 (.columnNames table-metadata)))

        (t/is (true? (.test page-idx-pred 0)))))))
