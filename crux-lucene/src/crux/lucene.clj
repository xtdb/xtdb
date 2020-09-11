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
           org.apache.lucene.index.DirectoryReader
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search IndexSearcher ScoreDoc]
           [org.apache.lucene.store Directory FSDirectory]))

(def ^:dynamic *node*)

(defrecord LuceneNode [directory analyzer index-writer-config]
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

(defn index-docs! [node docs]
  (let [{:keys [^Directory directory index-writer-config]} node]
    (with-open [iw (IndexWriter. directory, index-writer-config)]
      (doseq [doc docs]
        (.addDocument iw (l/crux-doc->lucene-doc doc))))))

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

(defmethod q/pred-args-spec 'text-search [_]
  (s/cat :pred-fn  #{'text-search} :args (s/spec (s/cat :attr q/literal? :v string?)) :return (s/? ::q/pred-return)))

(defmethod q/pred-constraint 'text-search [_ {:keys [encode-value-fn idx-id arg-bindings return-type] :as pred-ctx}]
  (let [[attr vval] (rest arg-bindings)]
    (fn pred-get-attr-constraint [index-store {:keys [entity-resolver-fn] :as db} idx-id->idx join-keys]
      (let [values (full-text *node* db attr vval)]
        (if (empty? values)
          false
          (do
            (q/bind-pred-result pred-ctx (get idx-id->idx idx-id) values)
            true))))))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defrecord CompactingIndexer [indexer document-store lucene-node]
  db/Indexer
  (index-docs [this docs]
    (db/index-docs indexer docs))
  (unindex-eids [this eids]
    (db/unindex-eids indexer eids))
  (index-entity-txs [this tx entity-txs]
    (let [content-hashes (entity-txes->content-hashes entity-txs)
          docs (vals (db/fetch-docs document-store content-hashes))]
      (index-docs! lucene-node docs)
      (db/index-entity-txs indexer tx entity-txs)))
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
        index-writer-config (IndexWriterConfig. analyzer)
        node (LuceneNode. directory analyzer index-writer-config)]
    (alter-var-root #'*node* (constantly node))))

(defn ->indexer
  {::sys/deps {:indexer :crux/indexer
               :document-store :crux/document-store
               :lucene-node ::node}}
  [{:keys [indexer document-store lucene-node]}]
  (CompactingIndexer. indexer document-store lucene-node))
