(ns crux.lucene
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.memory :as mem]
            [crux.query :as q]
            [crux.system :as sys])
  (:import crux.codec.EntityTx
           java.io.Closeable
           java.nio.file.Path
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field Field$Store StoredField StringField TextField]
           [org.apache.lucene.index DirectoryReader IndexWriter IndexWriterConfig Term]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder IndexSearcher Query ScoreDoc TermQuery]
           [org.apache.lucene.store Directory FSDirectory]))

(def ^:dynamic *node*)

(defrecord LuceneNode [directory analyzer]
  java.io.Closeable
  (close [this]
    (doseq [^Closeable c [directory]]
      (.close c))))

(defn- id->stored-bytes [eid]
  (mem/->on-heap (cc/->value-buffer eid)))

(defn- ^String eid->str [eid]
  (mem/buffer->hex (cc/->id-buffer eid)))

(defn- crux-doc->triples [crux-doc]
  (->> (dissoc crux-doc :crux.db/id)
       (mapcat (fn [[k v]]
                 (for [v (if (coll? v) v [v])
                       :when (string? v)]
                   [(name k) v])))))

(defrecord DocumentId [a v])

(defn- ^Document triple->doc [[k ^String v]]
  (doto (Document.)
    ;; To search for triples by eid-a-v for deduping
    (.add (StringField. "id", (eid->str (DocumentId. k v)), Field$Store/NO))
    ;; The Attr
    (.add (StringField. "attr", (name k), Field$Store/YES))
    ;; The actual term, which will be tokenized
    (.add (TextField. "val", v, Field$Store/YES))))

(defn- ^Term triple->term [[k ^String v]]
  (Term. "id" (eid->str (DocumentId. k v))))

(defn doc-count []
  (let [{:keys [^Directory directory]} *node*
        directory-reader (DirectoryReader/open directory)]
    (.numDocs directory-reader)))

(defn write-docs! [^IndexWriter index-writer docs]
  (doseq [d docs t (crux-doc->triples d)]
    (.updateDocument index-writer (triple->term t) (triple->doc t))))

(defn evict! [indexer, node, eids]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        attrs-id->attr (->> (db/read-index-meta indexer :crux/attribute-stats)
                            keys
                            (map #(vector (mem/buffer->hex (cc/->id-buffer %)) %))
                            (into {}))]
    (with-open [index-store (db/open-index-store indexer)
                index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
      (let [qs (for [[a v] (db/exclusive-avs indexer eids)
                     :let [a (attrs-id->attr (mem/buffer->hex a))
                           v (db/decode-value index-store v)]
                     :when (not= :crux.db/id a)]
                 (TermQuery. (Term. "id" (eid->str (DocumentId. (name a) v)))))]
        (.deleteDocuments index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs))))))

(defn search [node, k, v]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (QueryParser. "val" analyzer)
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))
        _ (when k
            (.add b (TermQuery. (Term. "attr" (name k))) BooleanClause$Occur/MUST))
        q (.build b)
        score-docs (.-scoreDocs (.search index-searcher q 1000))]
    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map (fn [^ScoreDoc d] (vector (.doc index-searcher (.-doc d)) (.-score d))) score-docs))))

(defn full-text [node db attr arg-v]
  (with-open [search-results ^crux.api.ICursor (search node attr arg-v)]
    (let [{:keys [index-store entity-resolver-fn]} db]
      (->> (iterator-seq search-results)
           (mapcat (fn [[^Document doc score]]
                     (let [v (.get ^Document doc "val")
                           a (or attr (keyword (.get ^Document doc "attr")))
                           encoded-v (db/encode-value index-store v)]
                       (for [eid (db/ave index-store a v nil entity-resolver-fn)]
                         [(db/decode-value index-store eid) v a score]))))
           (into [])))))

(defmethod q/pred-args-spec 'text-search [_]
  (s/cat :pred-fn  #{'text-search} :args (s/spec (s/cat :v string? :attr (s/? keyword?))) :return (s/? ::q/pred-return)))

(defmethod q/pred-constraint 'text-search [_ {:keys [encode-value-fn idx-id arg-bindings return-type] :as pred-ctx}]
  (let [[vval attr] (rest arg-bindings)]
    (fn pred-get-attr-constraint [index-store {:keys [entity-resolver-fn] :as db} idx-id->idx join-keys]
      (q/bind-pred-result pred-ctx (get idx-id->idx idx-id) (full-text *node* db attr vval)))))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defrecord LuceneDecoratingIndexer [indexer document-store lucene-node]
  db/Indexer
  (index-docs [this docs]
    (db/index-docs indexer docs))
  (unindex-eids [this eids]
    (try
      (evict! indexer lucene-node eids)
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
          (write-docs! index-writer docs)
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
  (LuceneDecoratingIndexer. indexer document-store lucene-node))
