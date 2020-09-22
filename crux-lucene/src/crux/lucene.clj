(ns crux.lucene
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.lucene :as l]
            [crux.memory :as mem]
            [crux.query :as q]
            [crux.system :as sys])
  (:import crux.codec.EntityTx
           java.io.Closeable
           java.nio.file.Path
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field StoredField TextField]
           [org.apache.lucene.index DirectoryReader IndexWriter IndexWriterConfig]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder IndexSearcher Query ScoreDoc]
           [org.apache.lucene.store Directory FSDirectory]
           org.apache.lucene.util.BytesRef))

(def ^:dynamic *node*)

(defrecord LuceneNode [directory analyzer]
  java.io.Closeable
  (close [this]
    (doseq [^Closeable c [directory]]
      (.close c))))

(defn- id->stored-bytes [eid]
  (mem/->on-heap (cc/->value-buffer eid)))

(defn crux-doc->lucene-docs [crux-doc]
  (->> (dissoc crux-doc :crux.db/id)
       (mapcat (fn [[k v]]
                 (for [v (if (coll? v) v [v])
                       :when (string? v)]
                   [(name k) v])))
       (map (fn [[k ^String v]]
              (doto (Document.)
                (.add (Field. "eid", ^String (.utf8ToString (BytesRef. ^bytes (id->stored-bytes (:crux.db/id crux-doc)))) TextField/TYPE_STORED))
                (.add (Field. "eid", ^bytes (id->stored-bytes (:crux.db/id crux-doc)) StoredField/TYPE))
                (.add (Field. (name k), v, TextField/TYPE_STORED)))))))

(defn- include? [{:keys [index-store entity-resolver-fn]} eid attr v]
  (let [encoded-v (db/encode-value index-store v)
        vs-in-crux (db/aev index-store attr eid encoded-v entity-resolver-fn)]
    (boolean (not-empty (filter (partial mem/buffers=? encoded-v) vs-in-crux)))))

(defn search [node, k, v]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (QueryParser. k analyzer)
        q (.build
           (doto (BooleanQuery$Builder.)
             (.add (.parse qp v) BooleanClause$Occur/MUST)))
        score-docs (.-scoreDocs (.search index-searcher q 10))]
    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map (fn [^ScoreDoc d] (vector (.doc index-searcher (.-doc d)) (.-score d))) score-docs))))

(defn delete! [node, eids]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        qp (QueryParser. "eid" analyzer)
        qs (map #(.parse qp (.utf8ToString (BytesRef. ^bytes (id->stored-bytes %)))) eids)]
    (with-open [index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
      (.deleteDocuments index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn full-text [node db attr arg-v]
  (with-open [search-results ^crux.api.ICursor (search node (name attr) arg-v)]
    (let [{:keys [index-store]} db]
      (->> (iterator-seq search-results)
           (keep (fn [[^Document doc score]]
                   (let [eid (mem/->off-heap (.-bytes (.getBinaryValue doc "eid")))
                         v (.get ^Document doc (name attr))]
                     (when (include? db eid attr v)
                       [(db/decode-value index-store eid) v score]))))
           (into [])))))

(defmethod q/pred-args-spec 'text-search [_]
  (s/cat :pred-fn  #{'text-search} :args (s/spec (s/cat :attr q/literal? :v string?)) :return (s/? ::q/pred-return)))

(defmethod q/pred-constraint 'text-search [_ {:keys [encode-value-fn idx-id arg-bindings return-type] :as pred-ctx}]
  (let [[attr vval] (rest arg-bindings)]
    (fn pred-get-attr-constraint [index-store {:keys [entity-resolver-fn] :as db} idx-id->idx join-keys]
      (q/bind-pred-result pred-ctx (get idx-id->idx idx-id) (full-text *node* db attr vval)))))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defrecord CompactingIndexer [indexer document-store lucene-node]
  db/Indexer
  (index-docs [this docs]
    (db/index-docs indexer docs))
  (unindex-eids [this eids]
    (try
      (delete! lucene-node eids)
      (catch Throwable t
        (clojure.tools.logging/error t)
        (throw t)))
    (db/unindex-eids indexer eids))
  (index-entity-txs [this tx entity-txs]
    (let [{:keys [^Directory directory ^Analyzer analyzer]} lucene-node
          index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
      (try
        (let [content-hashes (entity-txes->content-hashes entity-txs)
              docs (vals (db/fetch-docs document-store content-hashes))]
          (doseq [doc docs]
            (.addDocuments index-writer (l/crux-doc->lucene-docs doc)))
          (db/index-entity-txs indexer tx entity-txs)
          (.close index-writer))
        (catch Throwable t
          (clojure.tools.logging/error t)
          (.rollback index-writer)
          (throw t)))))
  (mark-tx-as-failed [this tx]
    (db/mark-tx-as-failed indexer tx))
  (store-index-meta [this k v]
    (db/store-index-meta indexer k v))
  (read-index-meta [this k]
    (db/read-index-meta indexer k))
  (read-index-meta [this k not-found]
    (db/read-index-meta indexer k not-found))
  (latest-completed-tx [this]
    (db/latest-completed-tx indexer))
  (tx-failed? [this tx-id]
    (db/tx-failed? indexer tx-id))
  (open-index-store ^java.io.Closeable [this]
    (db/open-index-store indexer)))

(defn ->node
  {::sys/args {:db-dir {:doc "Lucene DB Dir"
                        :required? true
                        :spec ::sys/path}}}
  [{:keys [^Path db-dir]}]
  (let [directory (FSDirectory/open db-dir)
        analyzer (StandardAnalyzer.)
        node (LuceneNode. directory analyzer)]
    (alter-var-root #'*node* (constantly node))))

(defn ->indexer
  {::sys/deps {:indexer :crux/indexer
               :document-store :crux/document-store
               :lucene-node ::node}}
  [{:keys [indexer document-store lucene-node]}]
  (CompactingIndexer. indexer document-store lucene-node))
