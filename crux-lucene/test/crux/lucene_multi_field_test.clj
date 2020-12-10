(ns crux.lucene-multi-field-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.api :as c]
            [crux.codec :as cc]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l]
            [crux.memory :as mem]
            [crux.query :as q])
  (:import org.apache.lucene.analysis.Analyzer
           [org.apache.lucene.document Document Field Field$Store StoredField TextField]
           org.apache.lucene.index.IndexWriter
           org.apache.lucene.queryparser.classic.QueryParser
           org.apache.lucene.search.Query))

(def ^:const ^:private field-content-hash "_crux_content_hash")
(def ^:const ^:private field-eid "_crux_eid")

(defn index-all-fields [[content-hash doc]]
  (let [d (Document.)]
    (.add d (StoredField. field-content-hash, ^bytes (mem/->on-heap (cc/->id-buffer content-hash))))
    (.add d (StoredField. field-eid, ^bytes (mem/->on-heap (cc/->value-buffer (:crux.db/id doc)))))
    (doseq [[k v] (filter (comp string? val) doc)]
      ;; The actual term, which will be tokenized
      (.add d (TextField. (l/keyword->k k), v, Field$Store/YES)))
    d))

(defn- index-docs! [^IndexWriter index-writer docs]
  (.addDocuments index-writer (map index-all-fields docs)))

(defn- evict! [index-store, ^IndexWriter index-writer, eids]
  (with-open [index-snapshot (db/open-index-snapshot index-store)]
    (doseq [eid eids]
      (println (db/entity-history index-snapshot eid :desc {}))))
  ;; get all content-hashes for an eid?
  #_(let [attrs-id->attr (->> (db/read-index-meta indexer :crux/attribute-stats)
                            keys
                            (map #(vector (->hash-str %) %))
                            (into {}))]
      (with-open [index-snapshot (db/open-index-snapshot indexer)]
      (let [qs (for [[a v] (db/exclusive-avs indexer eids)
                     :let [a (attrs-id->attr (->hash-str a))
                           v (db/decode-value index-snapshot v)]
                     :when (not= :crux.db/id a)]
                 (TermQuery. (Term. field-content-hash (->hash-str (DocumentId. a v)))))]
        (.deleteDocuments index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs))))))

(t/use-fixtures :each (lf/with-lucene-opts {:index-docs index-docs! :evict evict!}) fix/with-node)

(defn ^Query build-lucene-text-query
  [^Analyzer analyzer, [^String q]]
  (.parse (QueryParser. nil analyzer) q))

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
  (s/cat :pred-fn #{'lucene-text-search} :args (s/spec (s/cat :query string?)) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'lucene-text-search [_ pred-ctx]
  (l/pred-constraint #'build-lucene-text-query #'resolve-search-results-content-hash pred-ctx))

(t/deftest test-sanity-check
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "firstname: Fred") [[?e]]]
                                 [?e :crux.db/id]]}))))

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "firstname:James OR surname:smith") [[?e]]]
                                 [?e :crux.db/id]]})))
    (t/is (not (seq (c/q db {:find '[?e]
                             :where '[[(lucene-text-search "firstname:James OR surname:preston") [[?e]]]
                                      [?e :crux.db/id]]}))))))

(t/deftest test-namespaced-keywords
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :person/surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    ;; QueryParser/escape also works
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "person\\/surname: Smith") [[?e]]]
                                 [?e :crux.db/id]]})))))

(t/deftest test-evict
  (let [lucene-q "name: Smith"
        in-crux? (fn []
                   (with-open [db (c/open-db *api*)]
                     (boolean (seq (c/q db {:find '[?e]
                                            :where '[[(lucene-text-search "name: Smith") [[?e]]]
                                                     [?e :crux.db/id]]})))))
        in-lucene-store? (fn []
                           (let [lucene-store (:crux.lucene/lucene-store @(:!system *api*))]
                             (with-open [search-results (lf/search build-lucene-text-query ["name: Smith"])]
                               (boolean (seq (iterator-seq search-results))))))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Smith"}]])

    (assert (in-crux?))
    (assert (in-lucene-store?))

    (submit+await-tx [[:crux.tx/evict :ivan]])

    (t/is (not (in-crux?)))
    (t/is (not (in-lucene-store?)))))

;; todo test error handling, malformed query
