(ns crux.lucene
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.bus :as bus]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.memory :as mem]
            [crux.query :as q]
            [crux.system :as sys]
            [clojure.string])
  (:import crux.codec.EntityTx
           crux.query.VarBinding
           java.io.Closeable
           java.nio.file.Path
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field Field$Store StoredField StringField TextField]
           [org.apache.lucene.index DirectoryReader IndexWriter IndexWriterConfig Term]
           org.apache.lucene.queries.function.FunctionScoreQuery
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder DoubleValuesSource IndexSearcher Query ScoreDoc TermQuery]
           [org.apache.lucene.store Directory FSDirectory]))

(def ^:dynamic *lucene-store*)

(defrecord LuceneNode [directory analyzer]
  java.io.Closeable
  (close [this]
    (cio/try-close directory)))

(defn- id->stored-bytes [eid]
  (mem/->on-heap (cc/->value-buffer eid)))

(defn- ^String ->hash-str [eid]
  (str (cc/new-id eid)))

(defn- crux-doc->triples [crux-doc]
  (->> (dissoc crux-doc :crux.db/id)
       (mapcat (fn [[k v]]
                 (for [v (cc/vectorize-value v)
                       :when (string? v)]
                   [k v])))))

(defrecord DocumentId [a v])

(defn- ^String keyword->k [k]
  (subs (str k) 1))

(def ^:const ^:private field-crux-id "_crux_id")
(def ^:const ^:private field-crux-val "_crux_val")
(def ^:const ^:private field-crux-attr "_crux_attr")

(defn- ^Document triple->doc [[k ^String v]]
  (doto (Document.)
    ;; To search for triples by a-v for deduping
    (.add (StringField. field-crux-id, (->hash-str (DocumentId. k v)), Field$Store/NO))
    ;; The actual term, which will be tokenized
    (.add (TextField. (keyword->k k), v, Field$Store/YES))
    ;; Uses for wildcard searches
    (.add (TextField. field-crux-val, v, Field$Store/YES))
    ;; The Attr (storage only, for temporal resolution, could
    ;; potentially remove for non-wildcard usage)
    (.add (StringField. field-crux-attr, (keyword->k k), Field$Store/YES))))

(defn- ^Term triple->term [[k ^String v]]
  (Term. field-crux-id (->hash-str (DocumentId. k v))))

(defn doc-count []
  (let [{:keys [^Directory directory]} *lucene-store*
        directory-reader (DirectoryReader/open directory)]
    (.numDocs directory-reader)))

(defn- ^IndexWriter index-writer [lucene-store]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} lucene-store]
    (IndexWriter. directory, (IndexWriterConfig. analyzer))))

(defn- index-docs! [document-store lucene-store doc-ids]
  (try
    (let [docs (vals (db/fetch-docs document-store doc-ids))]
      (with-open [index-writer (index-writer lucene-store)]
        (doseq [d docs t (crux-doc->triples d)]
          (.updateDocument index-writer (triple->term t) (triple->doc t)))))
    (catch Throwable t
      ;; TODO until #1275 is fixed
      (log/error t)
      (throw t))))

(defn- evict! [indexer, node, eids]
  (let [{:keys [^Directory directory ^Analyzer analyzer]} node
        attrs-id->attr (->> (db/read-index-meta indexer :crux/attribute-stats)
                            keys
                            (map #(vector (->hash-str %) %))
                            (into {}))]
    (with-open [index-snapshot (db/open-index-snapshot indexer)
                index-writer (IndexWriter. directory, (IndexWriterConfig. analyzer))]
      (let [qs (for [[a v] (db/exclusive-avs indexer eids)
                     :let [a (attrs-id->attr (->hash-str a))
                           v (db/decode-value index-snapshot v)]
                     :when (not= :crux.db/id a)]
                 (TermQuery. (Term. field-crux-id (->hash-str (DocumentId. a v)))))]
        (.deleteDocuments index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs))))))

(defn- index-tx! [lucene-store tx]
  (let [t (Term. "meta" "latest-completed-tx")
        d (doto (Document.)
            (.add (StringField. "meta", "latest-completed-tx" Field$Store/NO))
            (.add (StoredField. "latest-completed-tx" ^long (:crux.tx/tx-id tx))))]
    (with-open [index-writer (index-writer lucene-store)]
      (.updateDocument index-writer t d))))

(defn latest-submitted-tx [lucene-store]
  (let [{:keys [^Directory directory]} lucene-store]
    (when (DirectoryReader/indexExists directory)
      (with-open [directory-reader (DirectoryReader/open directory)]
        (let [index-searcher (IndexSearcher. directory-reader)
              q (TermQuery. (Term. "meta" "latest-completed-tx"))
              d ^ScoreDoc (first (.-scoreDocs (.search index-searcher q 1)))]
          (when d (.get (.doc index-searcher (.-doc d)) "latest-completed-tx")))))))

(defn validate-lucene-store-up-to-date [index-store lucene-store]
  (when-let [latest-tx (db/latest-completed-tx index-store)]
    (let [latest-lucene-tx (latest-submitted-tx lucene-store)]
      (when-not (= latest-tx latest-lucene-tx)
        (throw (IllegalStateException. "Lucene store latest tx mismatch"))))))

(defn search [lucene-store, k, v]
  (assert lucene-store)
  (let [{:keys [^Directory directory ^Analyzer analyzer]} lucene-store
        directory-reader (DirectoryReader/open directory)
        index-searcher (IndexSearcher. directory-reader)
        qp (if k
             (QueryParser. (keyword->k k) analyzer)
             (QueryParser. field-crux-val analyzer))
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))
        q (.build b)
        q (FunctionScoreQuery. q (DoubleValuesSource/fromQuery q))
        score-docs (.-scoreDocs (.search index-searcher q 1000))]

    (when (seq score-docs)
      (log/debug (.explain index-searcher q (.-doc ^ScoreDoc (first score-docs)))))

    (cio/->cursor (fn []
                    (.close directory-reader))
                  (map (fn [^ScoreDoc d]
                         (vector (.doc index-searcher (.-doc d))
                                 (.-score d))) score-docs))))

(defn- full-text [node index-snapshot entity-resolver-fn attr arg-v]
  (when-not (string? arg-v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (with-open [search-results ^crux.api.ICursor (search node attr arg-v)]
    (->> (iterator-seq search-results)
         (mapcat (fn [[^Document doc score]]
                   (let [v (.get ^Document doc field-crux-val)
                         a (keyword (.get ^Document doc field-crux-attr))]
                     (for [eid (db/ave index-snapshot a v nil entity-resolver-fn)]
                       (if attr
                         [(db/decode-value index-snapshot eid) v score]
                         [(db/decode-value index-snapshot eid) v a score])))))
         (into []))))

(defn- pred-constraint [attr vval {:keys [encode-value-fn idx-id return-type tuple-idxs-in-join-order]}]
  (fn pred-get-attr-constraint [index-snapshot {:keys [entity-resolver-fn] :as db} idx-id->idx join-keys]
    (let [vval (if (instance? VarBinding vval)
                 (q/bound-result-for-var index-snapshot vval join-keys)
                 vval)]
      (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id) (full-text *lucene-store* index-snapshot entity-resolver-fn attr vval)))))

(defmethod q/pred-args-spec 'text-search [_]
  (s/cat :pred-fn  #{'text-search} :args (s/spec (s/cat :attr keyword? :v (some-fn string? symbol?))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'text-search [_ pred-ctx]
  (let [[attr vval] (rest (:arg-bindings pred-ctx))]
    (pred-constraint attr vval pred-ctx)))

(defmethod q/pred-args-spec 'wildcard-text-search [_]
  (s/cat :pred-fn  #{'wildcard-text-search} :args (s/spec (s/cat :v string?)) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'wildcard-text-search [_ pred-ctx]
  (let [[vval] (rest (:arg-bindings pred-ctx))]
    (pred-constraint nil vval pred-ctx)))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defn ->lucene-store
  {::sys/args {:db-dir {:doc "Lucene DB Dir"
                        :required? true
                        :spec ::sys/path}}
   ::sys/deps {:bus :crux/bus
               :document-store :crux/document-store
               :index-store :crux/index-store}}
  [{:keys [^Path db-dir index-store document-store bus] :as opts}]
  (let [directory (FSDirectory/open db-dir)
        analyzer (StandardAnalyzer.)
        lucene-store (LuceneNode. directory analyzer)]
    (validate-lucene-store-up-to-date index-store lucene-store)
    (alter-var-root #'*lucene-store* (constantly lucene-store))
    (bus/listen bus {:crux/event-types #{:crux.tx/indexing-tx-pre-commit :crux.tx/indexed-docs :crux.tx/unindexing-eids}
                     :crux.bus/executor (reify java.util.concurrent.Executor
                                          (execute [_ f]
                                            (.run f)))}
                (fn [ev]
                  (case (:crux/event-type ev)
                    :crux.tx/indexing-tx-pre-commit
                    (index-tx! lucene-store (:crux.tx/submitted-tx ev))
                    :crux.tx/indexed-docs
                    (index-docs! document-store lucene-store (:doc-ids ev))
                    :crux.tx/unindexing-eids
                    (evict! index-store lucene-store (:eids ev)))))
    lucene-store))
