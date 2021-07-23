(ns crux.lucene.multi-field
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.lucene :as l]
            [crux.memory :as mem]
            [crux.query :as q])
  (:import org.apache.lucene.analysis.Analyzer
           [org.apache.lucene.document Document Field Field$Store StoredField StringField TextField]
           [org.apache.lucene.index IndexWriter Term]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search Query TermQuery]))

(def ^:const ^:private field-content-hash "_crux_content_hash")
(def ^:const ^:private field-eid "_crux_eid")

(defrecord LuceneMultiFieldIndexer []
  l/LuceneIndexer

  (index! [this index-writer docs]
    (->> docs
         (map (fn [[content-hash doc]]
                (let [d (Document.)]
                  (.add d (StoredField. field-content-hash, ^bytes (mem/->on-heap (cc/->id-buffer content-hash))))
                  (.add d (StoredField. field-eid, ^bytes (mem/->on-heap (cc/->value-buffer (:crux.db/id doc)))))
                  (doseq [[k v] (filter (comp string? val) doc)]
                    ;; The actual term, which will be tokenized
                    (.add d (TextField. (l/keyword->k k), v, Field$Store/YES)))
                  ;; For eviction:
                  (.add d (StringField. field-eid, (str (cc/new-id (:crux.db/id doc))), Field$Store/NO))
                  d)))
         (.addDocuments ^IndexWriter index-writer)))

  (evict! [this index-writer eids]
    (doseq [eid eids
            :let [q (TermQuery. (Term. field-eid (str (cc/new-id eid))))]]
      (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query [q])))))

(defn ^Query build-lucene-text-query
  [^Analyzer analyzer, [q & args]]
  (when-not (string? q)
    (throw (IllegalArgumentException. "lucene-text-search query must be String")))
  (.parse (QueryParser. "" analyzer) (apply format q args)))

(defn- resolve-search-results-content-hash
  "Given search results each containing a content-hash, perform a
  temporal resolution to resolve the eid."
  [index-snapshot {:keys [entity-resolver-fn] :as db} search-results]
  (keep (fn [[^Document doc score]]
          (let [content-hash (mem/as-buffer (.-bytes (.getBinaryValue doc field-content-hash)))
                eid (cc/decode-value-buffer (mem/as-buffer (.-bytes (.getBinaryValue doc field-eid))))]
            (when (some-> (cc/->id-buffer eid) entity-resolver-fn (mem/buffers=? content-hash))
              [eid score])))
        search-results))

(defmethod q/pred-args-spec 'lucene-text-search [_]
  (s/cat :pred-fn #{'lucene-text-search} :args (s/spec (s/cat :query (some-fn string? q/logic-var?) :bindings (s/* (some-fn string? q/logic-var?)) :opts (s/? (some-fn map? q/logic-var?)))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'lucene-text-search [_ pred-ctx]
  (l/pred-constraint #'build-lucene-text-query #'resolve-search-results-content-hash pred-ctx))

(defn ->indexer
  [_]
  (LuceneMultiFieldIndexer.))
