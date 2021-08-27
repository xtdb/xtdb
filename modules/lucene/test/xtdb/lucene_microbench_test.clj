(ns xtdb.lucene-microbench-test
  (:require [clojure.test :as t]
            [xtdb.api :as c]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.lucene :as lf]
            [xtdb.fixtures.tpch :as tf]
            [xtdb.io :as xio]
            [xtdb.lucene :as l])
  (:import io.airlift.tpch.TpchTable
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.index IndexWriter IndexWriterConfig]
           org.apache.lucene.store.FSDirectory))

;; Lucene Ingest: 50000 docs (a/e/v) 7000 ms
;; Lucene Ingest: 50000 docs (a/v) 5000 ms

;; a/e/v (pred limit 1000)
;; Lucene Ingest: 6000 ms
;; Lucene Search: ~30 ms
;; Crux Ingest: 25000 ms
;; Crux Search: ~60 ms

;; a/v (pred limit 1000)
;; Lucene Ingest: 4500 ms
;; Lucene Search: ~10 ms
;; Crux Ingest: 25000 ms
;; Crux Search: ~60 ms

(defn customers [n]
  (take n (tf/tpch-table->docs (first (TpchTable/getTables)) {:scale-factor 0.5})))

(comment
  (let [tmp-dir (Files/createTempDirectory "lucene-temp" (make-array FileAttribute 0))]
    (try
      (with-open [directory (FSDirectory/open tmp-dir)]
        (let [analyzer (StandardAnalyzer.)]
          (time
           (let [index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
             (l/write-docs! index-writer (customers 50000))
             (.close index-writer)))
          (time
           (count (iterator-seq (l/search {:directory directory :analyzer analyzer} "c_comment" "ironic"))))))
      (finally
        (xio/delete-dir tmp-dir))))

  ((t/join-fixtures [(fix/with-opts {::l/lucene-store {}}) fix/with-node])
   (fn []
     (time
      (let [last-tx (->> (customers 50000)
                         (partition-all 1000)
                         (reduce (fn [last-tx chunk]
                                   (c/submit-tx *api* (vec (for [doc chunk]
                                                             [:xt/put doc]))))
                                 nil))]
        (c/await-tx *api* last-tx)))

     (time (count (c/q (c/db *api*) {:find '[?e]
                                     :where '[[(text-search :c_comment "ironic") [[?e]]]]}))))))

(comment
  ;; looking here at removing the IndexWriter and .commit overheads for tiny transactions (note: in general large 1000+ batch transactions will perform significantly better)

  ;; @9537073 (IndexWriter per tx) = very slow
  ;; IndexWriter + SearcherManager + .commit (per tx) = much faster
  ;; IndexWriter + SearcherManager = faster still, but we'll leave the .commit per tx where it is for now due to an inability for xtdb-lucene to recover from mismatched index scenarios, see the PR comments

  ((t/join-fixtures [(fix/with-opts {::l/lucene-store {}}) fix/with-node])
   (fn []
     (time
      (let [last-tx (->> (customers 1500)
                         (partition-all 1)
                         (reduce (fn [last-tx chunk]
                                   (c/submit-tx *api* (vec (for [doc chunk]
                                                             [:xt/put doc]))))
                                 nil))]
        (c/await-tx *api* last-tx))))))
