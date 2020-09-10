(ns crux.lucene
  (:require [crux.db :as db]
            [crux.io :as cio]
            [crux.lucene :as l]
            [crux.memory :as mem]
            [crux.node :as n]
            [crux.system :as sys])
  (:import crux.ByteUtils
           java.io.Closeable
           java.nio.file.Path
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           org.apache.lucene.document.Document
           org.apache.lucene.index.DirectoryReader
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search IndexSearcher ScoreDoc]
           [org.apache.lucene.store Directory FSDirectory]))

(defrecord LuceneNode [directory analyzer]
  java.io.Closeable
  (close [this]
    (doseq [^Closeable c [directory]]
      (.close c))))

(defn search [node, k, v]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} (:crux.lucene/node @(:!system node))
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (QueryParser. k analyzer)
        q (.parse qp v)
        score-docs (.-scoreDocs (.search index-searcher q 10))]
    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map #(.doc index-searcher (.-doc ^ScoreDoc %)) score-docs))))

;; Potential improvements / options:

;; option 2
;; Put the C into the doc
;; Simpler, docs don't benefit from structural sharing across Cs

;; option 3
;; Go to AVE, don't store eid or C in the index.
;; Requires sophisticated eviction, to use unindex-eids equivalent

(defn full-text [node db attr v]
  (with-open [search-results ^crux.api.ICursor (search node (name attr) v)]
    (let [{:keys [entity-resolver-fn index-store]} db
          eids (keep (fn [^Document doc]
                       (let [eid (mem/->off-heap (.-bytes (.getBinaryValue doc "eid")))
                             v (db/encode-value index-store (.get ^Document doc (name attr)))
                             vs-in-crux (db/aev index-store attr eid v entity-resolver-fn)]
                         (not-empty (filter (fn [v-in-crux]
                                              (= 0 (ByteUtils/compareBuffers v-in-crux v Integer/MAX_VALUE)))
                                            vs-in-crux))
                         eid))
                     (iterator-seq search-results))]
      (boolean (seq eids)))))

(defn ->node
  {::node {:start-fn start-lucene-node
           ::sys/args {:db-dir {:doc "Lucene DB Dir"
                                :required? true
                                :spec ::sys/path}}}}
  [{:keys [^Path db-dir]}]
  (let [directory (FSDirectory/open db-dir)]
    (LuceneNode. directory (StandardAnalyzer.))))
