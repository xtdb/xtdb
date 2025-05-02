(ns xtdb.metadata-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
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
           xtdb.api.storage.Storage
           xtdb.trie.Trie))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)
(t/use-fixtures :once tu/with-allocator)

(t/deftest test-param-metadata-error-310
  (xt/submit-tx tu/*node*
                [[:sql "INSERT INTO users (_id, name, _valid_from) VALUES (?, ?, ?)"
                  ["dave", "Dave", #inst "2018"]
                  ["claire", "Claire", #inst "2019"]]])

  (t/is (= [{:name "Dave"}]
           (xt/q tu/*node* ["SELECT users.name FROM users WHERE users._id = ?" "dave"]))
        "#310"))

(deftest test-bloom-filter-for-num-types-2133
  (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:num 0 :xt/id "a"}]
                               [:put-docs :xt_docs {:num 1 :xt/id "b"}]
                               [:put-docs :xt_docs {:num 1.0 :xt/id "c"}]
                               [:put-docs :xt_docs {:num 4 :xt/id "d"}]
                               [:put-docs :xt_docs {:num (short 3) :xt/id "e"}]
                               [:put-docs :xt_docs {:num 2.0 :xt/id "f"}]])
      (tu/then-await-tx tu/*node*))

  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (t/is (= [{:num 1} {:num 1.0}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{num (= num 1)}]]
                        {:node tu/*node*})))

  (t/is (= [{:num 2.0}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{num (= num 2)}]]
                        {:node tu/*node*})))

  (t/is (= [{:num 4}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{num (= num ?x)}]]
                        {:node tu/*node*, :args {:x (byte 4)}})))

  (t/is (= [{:num 3}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{num (= num ?x)}]]
                        {:node tu/*node*, :args {:x (float 3)}}))))

(deftest test-bloom-filter-for-datetime-types-2133
  (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:timestamp #xt/date "2010-01-01" :xt/id "a"}]
                               [:put-docs :xt_docs {:timestamp #xt/zoned-date-time "2010-01-01T00:00:00Z" :xt/id "b"}]
                               [:put-docs :xt_docs {:timestamp #xt/date-time "2010-01-01T00:00:00" :xt/id "c"}]
                               [:put-docs :xt_docs {:timestamp #xt/date "2020-01-01" :xt/id "d"}]]
                    {:default-tz #xt/zone "Z"})
      (tu/then-await-tx tu/*node*))

  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (t/is (= [{:timestamp #xt/date "2010-01-01"}
            {:timestamp #xt/zoned-date-time "2010-01-01T00:00Z"}
            {:timestamp #xt/date-time "2010-01-01T00:00:00"}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{timestamp (= timestamp #xt/zoned-date-time "2010-01-01T00:00:00Z")}]]
                        {:node tu/*node*, :default-tz #xt/zone "Z"})))

  (t/is (= [{:timestamp #xt/date "2010-01-01"}
            {:timestamp #xt/zoned-date-time "2010-01-01T00:00Z"}
            {:timestamp #xt/date-time "2010-01-01T00:00:00"}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{timestamp (= timestamp ?x)}]]
                        {:node tu/*node*, :default-tz #xt/zone "Z", :args {:x #xt/date "2010-01-01"}})))

  (t/is (= [{:timestamp #xt/date "2010-01-01"}
            {:timestamp #xt/zoned-date-time "2010-01-01T00:00Z"}
            {:timestamp #xt/date-time "2010-01-01T00:00:00"}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{timestamp (= timestamp #xt/date-time "2010-01-01T00:00:00")}]]
                        {:node tu/*node*, :default-tz #xt/zone "Z"}))))

(deftest test-bloom-filter-for-time-types
  (-> (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:time #xt/time "01:02:03" :xt/id "a"}]
                               [:put-docs :xt_docs {:time #xt/time "04:05:06" :xt/id "b"}]]
                    {:default-tz #xt/zone "Z"})
      (tu/then-await-tx tu/*node*))

  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (t/is (= [{:time #xt/time "04:05:06"}]
           (tu/query-ra '[:scan {:table public/xt_docs}
                          [{time (= time #xt/time "04:05:06")}]]
                        {:node tu/*node*, :default-tz #xt/zone "Z"}))))

(deftest test-min-max-on-xt-id
  (binding [c/*page-size* 16]
    (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:page-limit 16}}))]
      (-> (xt/submit-tx node (for [i (range 20)] [:put-docs :xt_docs {:xt/id i}]))
          (tu/then-await-tx node))

      (tu/finish-block! node)
      (c/compact-all! node #xt/duration "PT1S")

      (let [first-buckets (map (comp first tu/byte-buffer->path util/->iid) (range 20))
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

            relevant-pages (->> min-max-by-bucket
                                (filter (fn [[_ {:keys [min max]}]] (<= min 10 max)))
                                (map (comp bucket->page-idx first)))

            metadata-mgr (meta/<-node node)
            literal-selector (expr.meta/->metadata-selector tu/*allocator* '(and (< _id 11) (> _id 9)) '{_id :i64} vw/empty-args)]

        (t/testing "L0"
          (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l0-trie-key 0))]
            (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
              (let [page-idx-pred (.build literal-selector page-metadata)]

                (t/is (= #{"_iid" "_valid_to" "_valid_from" "_system_from" "_id"}
                         (.getColumnNames page-metadata)))

                (doseq [page-idx relevant-pages]
                  (t/is (true? (.test page-idx-pred page-idx))))))))

        (t/testing "L1"
          (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l1-trie-key nil 0))]
            (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
              (let [page-idx-pred (.build literal-selector page-metadata)]

                (t/is (= #{"_iid" "_valid_to" "_valid_from" "_id" "_system_from"}
                         (.getColumnNames page-metadata)))

                (doseq [page-idx relevant-pages]
                  (t/is (true? (.test page-idx-pred page-idx))
                        (str "page" page-idx)))))))))))

(deftest test-temporal-metadata
  (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:xt/id 1}]])

  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (let [metadata-mgr (meta/<-node tu/*node*)
        meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l0-trie-key 0))]
    (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
      (let [sys-time-micros (time/instant->micros #xt/instant "2020-01-01T00:00:00.000000Z")
            temporal-metadata (tu/->temporal-metadata sys-time-micros Long/MAX_VALUE sys-time-micros sys-time-micros)]
        (t/is (= temporal-metadata (.temporalMetadata page-metadata 0)))))))

(t/deftest test-boolean-metadata
  (xt/submit-tx tu/*node* [[:put-docs :xt_docs {:xt/id 1 :boolean-or-int true}]])
  (tu/finish-block! tu/*node*)

  (let [metadata-mgr (meta/<-node tu/*node*)
        true-selector (expr.meta/->metadata-selector tu/*allocator* '(= boolean_or_int true) '{boolean_or_int :bool} vw/empty-args)]

    (t/testing "L0"
      (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l0-trie-key 0))]
        (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
          (let [page-idx-pred (.build true-selector page-metadata)]
            (t/is (= #{"_iid" "_system_from" "_valid_from" "_valid_to" "_id" "boolean_or_int"}
                     (.getColumnNames page-metadata)))

            (t/is (true? (.test page-idx-pred 0)))))))

    (c/compact-all! tu/*node* #xt/duration "PT1S")

    (t/testing "L1"
      (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l1-trie-key nil 0))]
        (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
          (let [page-idx-pred (.build true-selector page-metadata)]
            (t/is (= #{"_iid" "_id" "_system_from" "_valid_from" "_valid_to" "boolean_or_int"}
                     (.getColumnNames page-metadata)))

            (t/is (true? (.test page-idx-pred 0)))))))))

(t/deftest test-set-metadata
  (let [node-dir (util/->path "target/test-set-metadata")]
    (util/delete-dir node-dir)
    (util/with-open [node (tu/->local-node {:node-dir node-dir})]

      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id "foo" :colours #{"red" "blue" "green"}}]])

      (tu/finish-block! node)
      (c/compact-all! node #xt/duration "PT1S")

      (let [metadata-mgr (meta/<-node node)]
        (t/testing "L0"
          (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l0-trie-key 0))]
            (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
              (tj/check-json (.toPath (io/as-file (io/resource "xtdb/metadata-test/set")))

                             (.resolve node-dir (str "objects/" Storage/STORAGE_ROOT "/tables/")))

              (t/is (= #{"_iid" "_system_from" "_valid_from" "_valid_to"
                         "_id" "$data$" "colours"}
                       (.getColumnNames page-metadata))))))

        (t/testing "L1"
          (let [meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l1-trie-key nil 0))]
            (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
              (tj/check-json (.toPath (io/as-file (io/resource "xtdb/metadata-test/set")))

                             (.resolve node-dir (str "objects/" Storage/STORAGE_ROOT "/tables/")))

              (t/is (= #{"_iid" "_id" "_system_from" "_valid_from" "_valid_to" "colours" "utf8"}
                       (.getColumnNames page-metadata))))))))))

(t/deftest test-duration-metadata-4198
  (let [node-dir (util/->path "target/test-duration-metadata")]
    (util/delete-dir node-dir)
    (util/with-open [node (tu/->local-node {:node-dir node-dir})]

      (xt/submit-tx node [[:put-docs :xt_docs
                           {:xt/id "foo", :duration #xt/duration "PT1H"}
                           {:xt/id "bar", :duration #xt/duration "P3D"}
                           {:xt/id "baz", :duration #xt/duration "PT0S"}]])

      (tu/finish-block! node)
      (c/compact-all! node #xt/duration "PT1S")

      (let [metadata-mgr (meta/<-node node)
            meta-file-path (Trie/metaFilePath "public$xt_docs" (trie/->l1-trie-key nil 0))]
        (util/with-open [page-metadata (.openPageMetadata metadata-mgr meta-file-path)]
          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/metadata-test/duration")))
                         (.resolve node-dir (str "objects/" Storage/STORAGE_ROOT "/tables/"))
                         #"l01.*")

          (t/is (= #{"_iid" "_id" "_system_from" "_valid_from" "_valid_to" "duration"}
                   (.getColumnNames page-metadata))))))))
