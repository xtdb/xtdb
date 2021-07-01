(ns crux.lucene
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.query :as q]
            [crux.system :as sys]
            [crux.tx :as tx]
            [crux.tx.conform :as txc]
            [crux.tx.event :as txe])
  (:import crux.query.VarBinding
           java.io.Closeable
           java.nio.file.Path
           java.time.Duration
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field$Store StoredField StringField TextField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig Term]
           org.apache.lucene.queries.function.FunctionScoreQuery
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder DoubleValuesSource IndexSearcher Query ScoreDoc SearcherManager TermQuery TopDocs]
           [org.apache.lucene.store Directory FSDirectory]))

(defrecord LuceneNode [directory analyzer index-writer searcher-manager indexer ^Thread fsync-thread]
  Closeable
  (close [_]
    (doto fsync-thread (.interrupt) (.join))
    (cio/try-close index-writer)
    (cio/try-close directory)))

(defn- ^String ->hash-str [eid]
  (str (cc/new-id eid)))

(defrecord DocumentId [e a v])

(defn ^String keyword->k [k]
  (subs (str k) 1))

(def ^:const ^:private field-crux-id "_crux_id")
(def ^:const ^:private field-crux-val "_crux_val")
(def ^:const ^:private field-crux-attr "_crux_attr")
(def ^:const ^:private field-crux-eid "_crux_eid")

(defn- ^IndexWriter ->index-writer [^Directory directory ^Analyzer analyzer]
  (IndexWriter. directory, (IndexWriterConfig. analyzer)))

(defn latest-completed-tx-id [^IndexWriter index-writer]
  (some-> (into {} (.getLiveCommitData index-writer))
          (get "crux.tx/tx-id")
          (Long/parseLong)))

(defn search [^SearcherManager searcher-manager, ^Query q]
  (let [^IndexSearcher index-searcher (.acquire searcher-manager)]
    (try
      (let [q (FunctionScoreQuery. q (DoubleValuesSource/fromQuery q))
            score-docs (letfn [(docs-page [after]
                                 (lazy-seq
                                  (let [^TopDocs
                                        top-docs (if after
                                                   (.searchAfter index-searcher after q 100)
                                                   (.search index-searcher q 100))
                                        score-docs (.-scoreDocs top-docs)]
                                    (concat score-docs
                                            (when (= 100 (count score-docs))
                                              (docs-page (last score-docs)))))))]
                         (docs-page nil))]

        (when (seq score-docs)
          (log/debug (.explain index-searcher q (.-doc ^ScoreDoc (first score-docs)))))

        (cio/->cursor (fn []
                        (.release searcher-manager index-searcher))
                      (->> score-docs
                           (map (fn [^ScoreDoc d]
                                  (vector (.doc index-searcher (.-doc d))
                                          (.-score d)))))))
      (catch Throwable t
        (.release searcher-manager index-searcher)
        (throw t)))))

(defn pred-constraint [query-builder results-resolver {:keys [arg-bindings idx-id return-type tuple-idxs-in-join-order ::lucene-store]}]
  (let [{:keys [searcher-manager]} lucene-store]
    (fn pred-get-attr-constraint [index-snapshot db idx-id->idx join-keys]
      (let [arg-bindings (map (fn [a]
                                (if (instance? VarBinding a)
                                  (q/bound-result-for-var index-snapshot a join-keys)
                                  a))
                              (rest arg-bindings))
            query (query-builder (:analyzer lucene-store) arg-bindings)
            tuples (with-open [search-results ^crux.api.ICursor (search searcher-manager query)]
                     (->> search-results
                          iterator-seq
                          (results-resolver index-snapshot db)
                          (into [])))]
        (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id) tuples)))))

(defn ^Query build-query
  "Standard build query fn, taking a single field/val lucene term string."
  [^Analyzer analyzer, [k v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (QueryParser. (keyword->k k) analyzer)
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defn resolve-search-results-a-v
  "Given search results each containing a single A/V pair document,
  perform a temporal resolution against A/V to resolve the eid."
  [attr index-snapshot {:keys [entity-resolver-fn] :as db} search-results]
  (mapcat (fn [[^Document doc score]]
            (let [v (.get ^Document doc field-crux-val)]
              (for [eid (doall (db/ave index-snapshot attr v nil entity-resolver-fn))]
                [(db/decode-value index-snapshot eid) v score])))
          search-results))

(defmethod q/pred-args-spec 'text-search [_]
  (s/cat :pred-fn  #{'text-search} :args (s/spec (s/cat :attr keyword? :v (some-fn string? symbol?))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'text-search [_ pred-ctx]
  (let [resolver (partial resolve-search-results-a-v (second (:arg-bindings pred-ctx)))]
    (pred-constraint #'build-query resolver pred-ctx)))

(defn- resolve-search-results-a-v-wildcard
  "Given search results each containing a single A/V pair document,
  perform a temporal resolution against A/V to resolve the eid."
  [index-snapshot {:keys [entity-resolver-fn] :as db} search-results]
  (mapcat (fn [[^Document doc score]]
            (let [v (.get ^Document doc field-crux-val)
                  a (keyword (.get ^Document doc field-crux-attr))]
              (for [eid (doall (db/ave index-snapshot a v nil entity-resolver-fn))]
                [(db/decode-value index-snapshot eid) v a score])))
          search-results))

(defn ^Query build-query-wildcard
  "Wildcard query builder"
  [^Analyzer analyzer, [v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (QueryParser. field-crux-val analyzer)
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defmethod q/pred-args-spec 'wildcard-text-search [_]
  (s/cat :pred-fn #{'wildcard-text-search} :args (s/spec (s/cat :v (some-fn string? symbol?))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'wildcard-text-search [_ pred-ctx]
  (pred-constraint #'build-query-wildcard #'resolve-search-results-a-v-wildcard pred-ctx))

(defprotocol LuceneIndexer
  (index! [this index-writer docs])
  (evict! [this index-writer eids]))

(defrecord LuceneAveIndexer []
  LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [{e :crux.db/id, :as crux-doc} (vals docs)
            [a v] (->> (dissoc crux-doc :crux.db/id)
                       (mapcat (fn [[a v]]
                                 (for [v (cc/vectorize-value v)
                                       :when (string? v)]
                                   [a v]))))
            :let [id-str (->hash-str (DocumentId. e a v))
                  doc (doto (Document.)
                        ;; To search for triples by e-a-v for deduping
                        (.add (StringField. field-crux-id, id-str, Field$Store/NO))
                        ;; The actual term, which will be tokenized
                        (.add (TextField. (keyword->k a), v, Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (TextField. field-crux-val, v, Field$Store/YES))
                        (.add (TextField. field-crux-eid, (->hash-str e), Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (StringField. field-crux-attr, (keyword->k a), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. field-crux-id id-str) doc)))

  (evict! [_ index-writer eids]
    (let [qs (for [eid eids]
               (TermQuery. (Term. field-crux-eid (->hash-str eid))))]
      (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn ->indexer [_]
  (LuceneAveIndexer.))

(defn ->analyzer [_]
  (StandardAnalyzer.))

(defn- transform-tx-events [document-store tx-events]
  (let [conformed-tx-events (map txc/<-tx-event tx-events)
        docs (db/fetch-docs document-store
                            (txc/conformed-tx-events->doc-hashes conformed-tx-events))]
    (reduce (fn [acc {:keys [op] :as tx-event}]
              (case op
                :crux.tx/evict (-> acc (update :evicted-eids conj (:eid tx-event)))
                :crux.tx/fn (let [{:keys [args-content-hash]} tx-event
                                  ;; doc replaced by this point
                                  nested-events (-> (db/fetch-docs document-store #{args-content-hash})
                                                    (get args-content-hash)
                                                    :crux.db.fn/tx-events)
                                  {:keys [docs evicted-eids]} (transform-tx-events document-store nested-events)]
                              {:docs (into (:docs acc) docs)
                               :evicted-eids (into (:evicted-eids acc) evicted-eids)})
                acc))
            {:docs docs
             :evicted-eids #{}}
            conformed-tx-events)))

(defn- fsync-loop [^IndexWriter index-writer ^Duration fsync-frequency]
  (log/debug "Starting Lucene fsync-loop...")
  (try
    (while true
      (try
        (Thread/sleep (.toMillis fsync-frequency))
        (log/debug "Committing Lucene IndexWriter...")
        (.commit index-writer)
        (log/debug "Committed Lucene IndexWriter.")

        (catch InterruptedException e
          (throw e))

        (catch Throwable t
          (log/warn t "error during Lucene IndexWriter commit"))))

    (catch InterruptedException _
      (log/debug "Stopped Lucene fsync-loop."))))

(defn ->lucene-store
  {::sys/args {:db-dir {:doc "Lucene DB Dir"
                        :required? true
                        :spec ::sys/path}
               :fsync-frequency {:required? true
                                 :spec ::sys/duration
                                 :default "PT5M"}}
   ::sys/deps {:document-store :crux/document-store
               :query-engine :crux/query-engine
               :indexer `->indexer
               :analyzer `->analyzer
               :secondary-indices :crux/secondary-indices}
   ::sys/before #{[:crux/tx-ingester]}}
  [{:keys [^Path db-dir document-store analyzer indexer query-engine secondary-indices fsync-frequency]}]
  (let [directory (FSDirectory/open db-dir)
        index-writer (->index-writer directory analyzer)
        searcher-manager (SearcherManager. index-writer false false nil)
        lucene-store (LuceneNode. directory analyzer
                                  index-writer searcher-manager
                                  indexer
                                  (doto (.newThread (cio/thread-factory "crux-lucene")
                                                    #(fsync-loop index-writer fsync-frequency))
                                    (.start)))]

    ;; Ensure lucene index exists for immediate queries:
    (.commit index-writer)
    (q/assoc-pred-ctx! query-engine ::lucene-store lucene-store)

    (tx/register-index! secondary-indices
                        (latest-completed-tx-id index-writer)
                        (fn [{:keys [::tx/tx-id ::txe/tx-events committing?]}]
                          (when committing?
                            (let [{:keys [docs evicted-eids]} (transform-tx-events document-store tx-events)]
                              (when-let [evicting-eids (not-empty evicted-eids)]
                                (evict! indexer index-writer evicting-eids))
                              (index! indexer index-writer docs)))

                          (.setLiveCommitData index-writer {"crux.tx/tx-id" (str tx-id)})
                          (.maybeRefreshBlocking searcher-manager)))

    lucene-store))
