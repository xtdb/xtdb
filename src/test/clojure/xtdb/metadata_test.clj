(ns xtdb.metadata-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.metadata :as meta]
            [xtdb.node :as xtn]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.metadata IMetadataManager)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)
(t/use-fixtures :once tu/with-allocator)

(t/deftest test-param-metadata-error-310
  (let [tx1 (xt/submit-tx tu/*node*
                          [[:sql "INSERT INTO users (_id, name, _valid_from) VALUES (?, ?, ?)"
                            ["dave", "Dave", #inst "2018"]
                            ["claire", "Claire", #inst "2019"]]])]

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* "SELECT users.name FROM users WHERE users._id = ?"
                   {:args ["dave"]
                    :basis {:at-tx tx1}}))
          "#310")))

(deftest test-bloom-filter-for-num-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:num 0 :xt/id "a"}]
                                        [:put-docs :xt_docs {:num 1 :xt/id "b"}]
                                        [:put-docs :xt_docs {:num 1.0 :xt/id "c"}]
                                        [:put-docs :xt_docs {:num 4 :xt/id "d"}]
                                        [:put-docs :xt_docs {:num (short 3) :xt/id "e"}]
                                        [:put-docs :xt_docs {:num 2.0 :xt/id "f"}]])
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:num 1} {:num 1.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 1)}]]
                          {:node tu/*node* :basis {:at-tx tx}})))

    (t/is (= [{:num 2.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 2)}]]
                          {:node tu/*node* :basis {:at-tx tx}})))

    (t/is (= [{:num 4}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:at-tx tx} :params {'?x (byte 4)}})))

    (t/is (= [{:num 3}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:at-tx tx} :params {'?x (float 3)}})))))

(deftest test-bloom-filter-for-datetime-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:timestamp #xt.time/date "2010-01-01" :xt/id "a"}]
                                        [:put-docs :xt_docs {:timestamp #xt.time/zoned-date-time "2010-01-01T00:00:00Z" :xt/id "b"}]
                                        [:put-docs :xt_docs {:timestamp #xt.time/date-time "2010-01-01T00:00:00" :xt/id "c"}]
                                        [:put-docs :xt_docs {:timestamp #xt.time/date "2020-01-01" :xt/id "d"}]]
                             {:default-tz #xt.time/zone "Z"})
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:timestamp #xt.time/date "2010-01-01"}
              {:timestamp #xt.time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #xt.time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #xt.time/zoned-date-time "2010-01-01T00:00:00Z")}]]
                          {:node tu/*node* :basis {:at-tx tx} :default-tz #xt.time/zone "Z"})))

    (t/is (= [{:timestamp #xt.time/date "2010-01-01"}
              {:timestamp #xt.time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #xt.time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp ?x)}]]
                          {:node tu/*node* :basis {:at-tx tx}
                           :default-tz  #xt.time/zone "Z" :params {'?x #xt.time/date "2010-01-01"}})))

    (t/is (= [{:timestamp #xt.time/date "2010-01-01"}
              {:timestamp #xt.time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #xt.time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #xt.time/date-time "2010-01-01T00:00:00")}]]
                          {:node tu/*node* :basis {:at-tx tx} :default-tz #xt.time/zone "Z"})))))

(deftest test-bloom-filter-for-time-types
  (let [tx (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:time #xt.time/time "01:02:03" :xt/id "a"}]
                                        [:put-docs :xt_docs {:time #xt.time/time "04:05:06" :xt/id "b"}]]
                             {:default-tz #xt.time/zone "Z"})
               (tu/then-await-tx tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:time #xt.time/time "04:05:06"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{time (= time #xt.time/time "04:05:06")}]]
                          {:node tu/*node* :basis {:at-tx tx} :default-tz #xt.time/zone "Z"})))))

(deftest test-min-max-on-xt-id
  (with-open [node (xtn/start-node {:indexer {:page-limit 16}})]
    (-> (xt/submit-tx node (for [i (range 20)] [:put-docs :xt_docs {:xt/id i}]))
        (tu/then-await-tx node))

    (tu/finish-chunk! node)

    (let [first-buckets (map (comp first tu/byte-buffer->path trie/->iid) (range 20))
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
          literal-selector (expr.meta/->metadata-selector '(and (< xt$id 11) (> xt$id 9)) '{xt$id :i64} vw/empty-params)
          meta-file-path (trie/->table-meta-file-path (util/->path "tables/xt_docs") (trie/->log-l0-l1-trie-key 0 0 21 20))]
      (util/with-open [table-metadata (.openTableMetadata metadata-mgr meta-file-path)]
        (let [page-idx-pred (.build literal-selector table-metadata)]

          (t/is (= #{"xt$iid" "xt$valid_to" "xt$valid_from" "xt$id" "xt$system_from"}
                   (.columnNames table-metadata)))

          (doseq [page-idx relevant-pages]
            (t/is (true? (.test page-idx-pred page-idx)))))))))

(deftest test-temporal-metadata
  (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:xt/id 1}]])

  (tu/finish-chunk! tu/*node*)

  (let [^IMetadataManager metadata-mgr (tu/component tu/*node* ::meta/metadata-manager)
        meta-file-path (trie/->table-meta-file-path (util/->path "tables/xt_docs") (trie/->log-l0-l1-trie-key 0 0 2 1))]
    (util/with-open [table-metadata (.openTableMetadata metadata-mgr meta-file-path)]
      (let [sys-time-micros (time/instant->micros #xt.time/instant "2020-01-01T00:00:00.000000Z")
            temporal-dimension (TemporalDimension. sys-time-micros Long/MAX_VALUE)
            metadata-bounds (TemporalBounds. temporal-dimension temporal-dimension)]
        (t/is (= metadata-bounds (.temporalBounds table-metadata 0)))))))

(t/deftest test-boolean-metadata
  (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:xt/id 1 :boolean-or-int true}]])
  (tu/finish-chunk! tu/*node*)

  (let [^IMetadataManager metadata-mgr (tu/component tu/*node* ::meta/metadata-manager)
        true-selector (expr.meta/->metadata-selector '(= boolean-or-int true) '{boolean-or-int :bool} vw/empty-params)
        meta-file-path (trie/->table-meta-file-path (util/->path "tables/xt_docs") (trie/->log-l0-l1-trie-key 0 0 2 1))]

    (util/with-open [table-metadata (.openTableMetadata metadata-mgr meta-file-path)]
      (let [page-idx-pred (.build true-selector table-metadata)]
        (t/is (= #{"xt$iid" "xt$id" "xt$system_from" "xt$valid_from" "xt$valid_to" "boolean_or_int"}
                 (.columnNames table-metadata)))

        (t/is (true? (.test page-idx-pred 0)))))))

(t/deftest test-set-metadata
  (let [node-dir (util/->path "target/test-set-metadata")]
    (util/delete-dir node-dir)
    (util/with-open [node (tu/->local-node {:node-dir node-dir})]

      (xt/submit-tx node [[:put-docs :xt_docs {:xt/id "foo" :colours #{"red" "blue" "green"}}]])

      (tu/finish-chunk! node)

      (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)
            meta-file-path (trie/->table-meta-file-path (util/->path "tables/xt_docs/") (trie/->log-l0-l1-trie-key 0 0 2 1))]

        (util/with-open [table-metadata (.openTableMetadata metadata-mgr meta-file-path)]
          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/metadata-test/set")))

                         (.resolve node-dir (str "objects/" bp/version "/tables/")))

          (t/is (= #{"xt$iid" "xt$id" "xt$system_from" "xt$valid_from" "xt$valid_to" "colours" "$data$"}
                   (.columnNames table-metadata))))))))
