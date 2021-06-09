(ns crux.lucene
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.bus :as bus]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.query :as q]
            [crux.system :as sys])
  (:import clojure.lang.IFn
           crux.query.VarBinding
           java.io.Closeable
           java.io.IOException
           java.nio.file.Path
           java.time.Duration
           java.util.Date
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field$Store StoredField StringField TextField]
           [org.apache.lucene.index DirectoryReader IndexWriter IndexWriterConfig Term]
           org.apache.lucene.queries.function.FunctionScoreQuery
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder DoubleValuesSource IndexSearcher Query ScoreDoc SearcherManager TermQuery TopDocs]
           [org.apache.lucene.store Directory FSDirectory]
           [java.util.concurrent Future Executors TimeUnit]))

(defn throttle [name f wait-ms]
  (if (= wait-ms 0)
    (reify
      IFn
      (invoke [_]
        (f)
        nil)
      Closeable
      (close [_]))
    (let [!state (atom [(.getTime (Date.)) false])
          executor (Executors/newSingleThreadScheduledExecutor (cio/thread-factory name))
          run-f (fn run-f [now]
                  (reset! !state [(or now (.getTime (Date.))) false])
                  (f))
          schedule-run (fn run []
                          (let [[last-run-time fut] @!state]
                            (when (false? fut)
                              (let [now (.getTime (Date.))
                                    next-run (+ last-run-time wait-ms)]
                                (if (< next-run now)
                                  (run-f now)
                                  (reset! !state [last-run-time
                                                  (.schedule executor
                                                             ^Runnable (partial run-f false)
                                                             ^long (- next-run now)
                                                             TimeUnit/MILLISECONDS)]))))))]
      (reify
        IFn
        (invoke [_]
          (let [[_ fut] @!state]
            (when-not (true? fut)
              (schedule-run)
              nil)))
        Closeable
        (close [_]
          (let [[_ fut] @!state]
            (when-not (true? fut)
              (when-not (false? fut)
                (.get ^Future fut))
              (doto executor
                (.shutdownNow)
                (.awaitTermination 5000 TimeUnit/MILLISECONDS))
              (reset! !state [(.getTime (Date.)) true]))))))))

(defrecord LuceneNode [directory analyzer index-writer searcher-manager indexer fsync-executor]
  Closeable
  (close [this]
    (.close ^IndexWriter index-writer)
    (.close ^SearcherManager searcher-manager)
    (.close ^Closeable fsync-executor)
    (cio/try-close directory)))

(defn- ^String ->hash-str [eid]
  (str (cc/new-id eid)))

(defrecord DocumentId [a v])

(defn ^String keyword->k [k]
  (subs (str k) 1))

(def ^:const ^:private field-crux-id "_crux_id")
(def ^:const ^:private field-crux-val "_crux_val")
(def ^:const ^:private field-crux-attr "_crux_attr")

(defn- ^IndexWriter ->index-writer [^Directory directory ^Analyzer analyzer]
  (IndexWriter. directory, (IndexWriterConfig. analyzer)))

(defn- index-tx! [^IndexWriter index-writer tx]
  (let [t (Term. "meta" "latest-completed-tx")
        d (doto (Document.)
            (.add (StringField. "meta", "latest-completed-tx" Field$Store/NO))
            (.add (StoredField. "latest-completed-tx" ^long (:crux.tx/tx-id tx))))]
    (.updateDocument index-writer t d)))

(defn- ->index-searcher [lucene-store]
  (let [^SearcherManager searcher-manager (:searcher-manager lucene-store)
        s (.acquire searcher-manager)]
    [s (fn [] (.release searcher-manager s))]))

(defn latest-completed-tx [lucene-store]
  (let [[^IndexSearcher index-searcher index-searcher-release-fn] (->index-searcher lucene-store)]
    (try
      (let [q (TermQuery. (Term. "meta" "latest-completed-tx"))]
        (when-let [^ScoreDoc d (first (.-scoreDocs (.search index-searcher q 1)))]
          (some-> (.doc index-searcher (.-doc d))
                  (.get "latest-completed-tx")
                  (Long/parseLong))))
      (finally
        (index-searcher-release-fn)))))

(defn validate-lucene-store-up-to-date [index-store lucene-store]
  (let [{:crux.tx/keys [tx-id] :as latest-tx} (db/latest-completed-tx index-store)
        latest-lucene-tx-id (latest-completed-tx lucene-store)]
    (when (and tx-id
               (or (nil? latest-lucene-tx-id)
                   (> tx-id latest-lucene-tx-id)))
      (throw (IllegalStateException. "Lucene store latest tx mismatch")))))

(defn search [lucene-store, ^Query q]
  (assert lucene-store)
  (let [[^IndexSearcher index-searcher index-searcher-release-fn] (->index-searcher lucene-store)]
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
                        (index-searcher-release-fn))
                      (->> score-docs
                           (map (fn [^ScoreDoc d]
                                  (vector (.doc index-searcher (.-doc d))
                                          (.-score d)))))))
      (catch Throwable t
        (index-searcher-release-fn)
        (throw t)))))

(defn pred-constraint [query-builder results-resolver {:keys [arg-bindings idx-id return-type tuple-idxs-in-join-order ::lucene-store]}]
  (fn pred-get-attr-constraint [index-snapshot db idx-id->idx join-keys]
    (let [arg-bindings (map (fn [a]
                              (if (instance? VarBinding a)
                                (q/bound-result-for-var index-snapshot a join-keys)
                                a))
                            (rest arg-bindings))
          query (query-builder (:analyzer lucene-store) arg-bindings)
          tuples (with-open [search-results ^crux.api.ICursor (search lucene-store query)]
                   (->> search-results
                        iterator-seq
                        (results-resolver index-snapshot db)
                        (into [])))]
      (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id) tuples))))

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

(defrecord LuceneAvIndexer [index-store]
  LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [crux-doc (vals docs)
            [k v] (->> (dissoc crux-doc :crux.db/id)
                       (mapcat (fn [[k v]]
                                 (for [v (cc/vectorize-value v)
                                       :when (string? v)]
                                   [k v]))))
            :let [id-str (->hash-str (DocumentId. k v))
                  doc (doto (Document.)
                        ;; To search for triples by a-v for deduping
                        (.add (StringField. field-crux-id, id-str, Field$Store/NO))
                        ;; The actual term, which will be tokenized
                        (.add (TextField. (keyword->k k), v, Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (TextField. field-crux-val, v, Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (StringField. field-crux-attr, (keyword->k k), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. field-crux-id id-str) doc)))

  (evict! [_ index-writer eids]
    (with-open [index-snapshot (db/open-index-snapshot index-store)]
      (let [attrs-id->attr (->> (db/all-attrs index-snapshot)
                                (map #(vector (->hash-str %) %))
                                (into {}))
            qs (for [[a v] (db/exclusive-avs index-store eids)
                     :let [a (attrs-id->attr (->hash-str a))
                           v (db/decode-value index-snapshot v)]
                     :when (not= :crux.db/id a)]
                 (TermQuery. (Term. field-crux-id (->hash-str (DocumentId. a v)))))]
        (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs))))))

(defn ->indexer
  {::sys/deps {:index-store :crux/index-store}}
  [{:keys [index-store]}]
  (LuceneAvIndexer. index-store))

(defn ->analyzer
  [_]
  (StandardAnalyzer.))

(defn ->lucene-store
  {::sys/args {:db-dir {:doc "Lucene DB Dir"
                        :required? true
                        :spec ::sys/path}
               :fsync-throttle {:doc "Minimum interval between fsync'd Lucene commits (0 is synchronous)"
                                :required? false
                                :default (Duration/ofSeconds 0)
                                :spec ::sys/duration}}
   ::sys/deps {:bus :crux/bus
               :document-store :crux/document-store
               :index-store :crux/index-store
               :query-engine :crux/query-engine
               :indexer `->indexer
               :analyzer `->analyzer}
   ::sys/before #{[:crux/tx-ingester]}}
  [{:keys [^Path db-dir index-store document-store bus analyzer indexer query-engine ^Duration fsync-throttle]}]
  (let [directory (FSDirectory/open db-dir)
        index-writer (->index-writer directory analyzer)
        searcher-manager (SearcherManager. index-writer false false nil)
        fsync-executor (throttle "crux-lucene-fsync-throttler"
                                 (fn []
                                   (.maybeRefreshBlocking searcher-manager)
                                   (try
                                     (.commit index-writer)
                                     (catch IOException e
                                       (log/error e "Error during Lucene commit:"))))
                                 (.toMillis fsync-throttle))
        lucene-store (LuceneNode. directory analyzer index-writer searcher-manager indexer fsync-executor)]
    (fsync-executor) ; Ensure Lucene index exists for immediate queries
    (validate-lucene-store-up-to-date index-store lucene-store)
    (q/assoc-pred-ctx! query-engine ::lucene-store lucene-store)
    (bus/listen bus {:crux/event-types #{:crux.tx/committing-tx :crux.tx/aborting-tx}
                     :crux.bus/executor (reify java.util.concurrent.Executor
                                          (execute [_ f]
                                            (.run f)))}
                (fn [ev]
                  (when (= :crux.tx/committing-tx (:crux/event-type ev))
                    (index! indexer index-writer (db/fetch-docs document-store (:doc-ids ev)))
                    (when-let [evicting-eids (not-empty (:evicting-eids ev))]
                      (evict! indexer index-writer evicting-eids)))
                  (index-tx! index-writer (:submitted-tx ev))
                  (.maybeRefreshBlocking searcher-manager)
                  (fsync-executor)))
    lucene-store))
