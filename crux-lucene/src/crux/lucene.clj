(ns crux.lucene
  (:require [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.lucene :as l]
            [clojure.spec.alpha :as s]
            [crux.memory :as mem]
            [crux.query :as q]
            [crux.system :as sys])
  (:import java.io.Closeable
           java.nio.file.Path
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field StoredField TextField]
           org.apache.lucene.index.DirectoryReader
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search IndexSearcher ScoreDoc]
           [org.apache.lucene.store Directory FSDirectory]))

(def ^:dynamic *node*)

(defrecord LuceneNode [directory analyzer]
  java.io.Closeable
  (close [this]
    (doseq [^Closeable c [directory]]
      (.close c))))

(defn crux-doc->lucene-doc [crux-doc]
  (let [doc (doto (Document.)
              (.add (Field. "eid", (mem/->on-heap (cc/->value-buffer (:crux.db/id crux-doc))) StoredField/TYPE)))]
    (reduce-kv (fn [^Document doc k v]
                 (when (string? v)
                   (.add doc (Field. (name k), ^String v, TextField/TYPE_STORED)))
                 doc)
               doc
               (dissoc crux-doc :crux.db/id))))

(defn search [node, k, v]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (QueryParser. k analyzer)
        q (.parse qp v)
        score-docs (.-scoreDocs (.search index-searcher q 10))]
    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map #(.doc index-searcher (.-doc ^ScoreDoc %)) score-docs))))

(defn full-text [node db attr v]
  (with-open [search-results ^crux.api.ICursor (search node (name attr) v)]
    (let [{:keys [entity-resolver-fn index-store]} db
          eids (keep (fn [^Document doc]
                       (let [eid (mem/->off-heap (.-bytes (.getBinaryValue doc "eid")))
                             v (db/encode-value index-store (.get ^Document doc (name attr)))
                             vs-in-crux (db/aev index-store attr eid v entity-resolver-fn)]
                         (not-empty (filter (partial mem/buffers=? v) vs-in-crux))
                         eid))
                     (iterator-seq search-results))]
      (map #(vector (db/decode-value index-store %)) eids))))

(defmethod q/pred-constraint 'text-search [_ {:keys [encode-value-fn idx-id arg-bindings return-type] :as pred-ctx}]
  (let [[attr vval] (rest arg-bindings)]
    (fn pred-get-attr-constraint [index-store {:keys [entity-resolver-fn] :as db} idx-id->idx join-keys]
      (let [values (full-text *node* db attr vval)]
        (if (empty? values)
          false
          (do
            (q/bind-pred-result pred-ctx (get idx-id->idx idx-id) values)
            true))))))

(defn ->node
  {::node {:start-fn start-lucene-node
           ::sys/args {:db-dir {:doc "Lucene DB Dir"
                                :required? true
                                :spec ::sys/path}}}}
  [{:keys [^Path db-dir]}]
  (let [directory (FSDirectory/open db-dir)
        node (LuceneNode. directory (StandardAnalyzer.))]
    (alter-var-root #'*node* (constantly node))))


;; Potential improvements / options:

;; option 2
;; Put the C into the doc
;; Simpler, docs don't benefit from structural sharing across Cs

;; option 3
;; Go to AVE, don't store eid or C in the index.
;; Requires sophisticated eviction, to use unindex-eids equivalent
