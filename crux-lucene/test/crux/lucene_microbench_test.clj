(ns crux.lucene-microbench-test
  (:require [crux.api :as c]
            [crux.fixtures :as fix]
            [crux.fixtures.tpch :as tf]
            [crux.io :as cio]
            [crux.lucene :as l])
  (:import io.airlift.tpch.TpchTable
           java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.index IndexWriter IndexWriterConfig]
           org.apache.lucene.store.FSDirectory))

;; Testing with 1k docs (TPCH Customers)
;; 50000 docs (a/e) 4500 ms
;; Search term: ~10 ms

(declare node)

(defn customers [n]
  (take n (tf/tpch-table->docs (first (TpchTable/getTables)) {:scale-factor 0.5})))

(comment

  (def dir (.toFile (Files/createTempDirectory "microbench" (make-array FileAttribute 0))))

  (def node (c/start-node {}))
  (def node (c/start-node {::l/node {:db-dir (.toPath ^java.io.File dir)}
                           :crux/indexer {:crux/module 'crux.lucene/->indexer
                                          :indexer 'crux.kv.indexer/->kv-indexer}}))

  ;; 1000 customers:
  ;; ~450 millis
  ;; ~550 millis - lucene

  (time
   (count (fix/transact! node (customers 1000))))

  (.close node)

  ;; 1k ingest

  (let [tmp-dir (Files/createTempDirectory "lucene-temp" (make-array FileAttribute 0))]
    (println (count (customers 50000)))
    (try
      (with-open [directory (FSDirectory/open tmp-dir)]
        (let [analyzer (StandardAnalyzer.)]
          (time
           (let [index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
             (l/write-docs! index-writer (customers 50000))
             (.close index-writer)))

          (time
           (count (iterator-seq (l/search {:directory directory :analyzer analyzer} "c_comment" "some awful"))))))
      (finally
        (cio/delete-dir tmp-dir)))))


;; Hard to reason about reusing docs - we can't reuse them in a transaction. Need to take out a pool of them
;; See https://cwiki.apache.org/confluence/display/lucene/ImproveIndexingSpeed

;; try next querying, have a play with the AV lookup
;;; Do a roundtrup ingest then n queries - compare AV vs EAV
