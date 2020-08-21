(ns crux.lucene
  (:require [crux.io :as cio])
  (:import java.io.Closeable
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
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
  (let [{:keys [^Analyzer analyzer ^Directory directory]} (:crux.lucene/node (:crux.node/topology (meta node)))
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (QueryParser. k analyzer)
        q (.parse qp v)
        score-docs (.-scoreDocs (.search index-searcher q 10))]
    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map #(.doc index-searcher (.-doc ^ScoreDoc %)) score-docs))))

(defn- start-lucene-node [_ {::keys [db-dir]}]
  (let [directory (FSDirectory/open db-dir)]
    (LuceneNode. directory (StandardAnalyzer.))))

(def module {::node {:start-fn start-lucene-node
                     :args {::db-dir {:doc "Lucene DB Dir"
                                      :crux.config/type :crux.config/path}}}})
