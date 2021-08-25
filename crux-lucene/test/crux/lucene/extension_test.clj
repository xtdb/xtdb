(ns crux.lucene.extension-test
  (:require  [clojure.test :as t]
             [crux.api :as c]
             [crux.codec :as cc]
             [crux.system :as sys]
             [crux.fixtures :as fix :refer [*api* submit+await-tx]]
             [crux.lucene :as l])
  (:import org.apache.lucene.analysis.Analyzer
           org.apache.lucene.queries.function.FunctionScoreQuery
           [org.apache.lucene.analysis.core KeywordAnalyzer KeywordTokenizerFactory LowerCaseFilterFactory]
           [org.apache.lucene.analysis.standard StandardTokenizerFactory]
           org.apache.lucene.analysis.custom.CustomAnalyzer
           org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
           [org.apache.lucene.document Document Field$Store StringField TextField]
           [org.apache.lucene.index IndexWriter Term]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder TermQuery Query DoubleValuesSource]))

(require 'crux.lucene.multi-field :reload) ; for dev, due to changing protocols

(t/use-fixtures :each (fix/with-opts {::l/lucene-store {}}) fix/with-node)

(t/deftest test-multiple-lucene-stores
  (with-open [node (c/start-node {:eav {:xt/module 'crux.lucene/->lucene-store
                                        :analyzer 'crux.lucene/->analyzer
                                        :indexer 'crux.lucene/->indexer}
                                  :multi {:xt/module 'crux.lucene/->lucene-store
                                          :analyzer 'crux.lucene/->analyzer
                                          :indexer 'crux.lucene.multi-field/->indexer}})]
    (submit+await-tx node [[:xt/put {:xt/id :ivan
                                     :firstname "Fred"
                                     :surname "Smith"}]])
    (with-open [db (c/open-db node)]
      (t/is (seq (c/q db '{:find [?e]
                           :where [[(text-search :firstname "Fre*" {:lucene-store-k :eav}) [[?e]]]
                                   [(lucene-text-search "firstname:%s AND surname:%s" "Fred" "Smith" {:lucene-store-k :multi}) [[?e]]]]}))))))

(t/deftest test-userspace-text-search-limit
  (submit+await-tx (for [n (range 10)] [:xt/put {:xt/id n, :description (str "Entity " n)}]))
  (submit+await-tx (for [n (range 4)] [:xt/put {:xt/id n, :description (str "Entity v2 " n)}]))
  (submit+await-tx (for [n (range 4)] [:xt/put {:xt/id (+ n 4), :description (str "Entity v2 " n)}]))

  (with-open [db (c/open-db *api*)]
    (with-open [s (l/search *api* "Entity*" {:default-field (name :description)})]
      (t/is (= 5 (count (->> (iterator-seq s)
                             (map (fn doc->rel [[^Document doc score]]
                                    [(.get ^Document doc l/field-xt-val) score]))
                             (distinct) ;; rely on the later resolve step to find all the entities sharing a given AV
                             (mapcat (fn resolve-atemporal-av [[v s]]
                                       (c/q db {:find '[e v s]
                                                :in '[v s]
                                                :where [['e :description 'v]]}
                                            v s)))
                             (take 5))))))))

(t/deftest test-userspace-wildcard-text-search-limit
  (submit+await-tx (for [n (range 10)] [:xt/put {:xt/id n, :description (str "Entity " n) :foo (str "Entitybar" n)}]))
  (submit+await-tx (for [n (range 4)] [:xt/put {:xt/id n, :description (str "Entity v2 " n) :foo (str "Entitybaz" n)}]))
  (submit+await-tx (for [n (range 4)] [:xt/put {:xt/id (+ n 4), :description (str "Entity v2 " n) :foo (str "Entityqaz" n)}]))

  (with-open [db (c/open-db *api*)]
    (with-open [s (l/search *api* "Entity*" {})]
      (t/is (= 5 (count (->> (iterator-seq s)
                             (map (fn doc->rel [[^Document doc score]]
                                    [(.get ^Document doc l/field-xt-attr) (.get ^Document doc l/field-xt-val) score]))
                             (distinct) ;; rely on the later resolve step to find all the entities sharing a given AV
                             (mapcat (fn resolve-atemporal-av [[a v s]]
                                       (c/q db {:find '[e a v s]
                                                :in '[a v s]
                                                :where [['e (keyword a) 'v]]}
                                            (keyword a) v s)))
                             (take 5))))))))

;;; Egeria Connector use-case, derived from https://github.com/odpi/egeria-connector-crux/

(def ^:const field-xt-val-exact "_crux_val_exact")

(defn ^String keyword->kcs [k]
  (subs (str k "-exact") 1))

(defrecord CustomEavIndexer [index-store]
  l/LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [{e :xt/id, :as crux-doc} (vals docs)
            [a ^String v] (->> (dissoc crux-doc :xt/id)
                               (mapcat (fn [[a v]]
                                         (for [v (cc/vectorize-value v)
                                               :when (string? v)]
                                           [a v]))))
            :let [id-str (l/->hash-str (l/->DocumentId e a v))
                  doc (doto (Document.)
                        ;; To search for triples by e-a-v for deduping
                        (.add (StringField. l/field-xt-id, id-str, Field$Store/NO))
                        ;; The actual term, which will be tokenized (case-insensitive) (dynamic field)
                        (.add (TextField. (l/keyword->k a), v, Field$Store/YES))
                        ;; Custom - the actual term, to be used for exact matches (case-sensitive) (dynamic field)
                        (.add (StringField. (keyword->kcs a), v, Field$Store/YES))
                        ;; Used for wildcard searches (case-insensitive)
                        (.add (TextField. l/field-xt-val, v, Field$Store/YES))
                        ;; Custom - used for wildcard searches (case-sensitive)
                        (.add (StringField. field-xt-val-exact, v, Field$Store/YES))
                        ;; Used for eviction
                        (.add (StringField. l/field-xt-eid, (l/->hash-str e), Field$Store/NO))
                        ;; Used for wildcard searches
                        (.add (StringField. l/field-xt-attr, (l/keyword->k a), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. l/field-xt-id id-str) doc)))

  (evict! [_ index-writer eids]
    (let [qs (for [eid eids]
               (TermQuery. (Term. l/field-xt-eid (l/->hash-str eid))))]
      (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn ->custom-eav-indexer
  {::sys/deps {:index-store :xt/index-store}}
  [{:keys [index-store]}]
  (CustomEavIndexer. index-store))

(defn ^Analyzer ->custom-analyzer
  "case-insensitive analyzer"
  [_]
  (.build (doto (CustomAnalyzer/builder)
            (.withTokenizer ^String KeywordTokenizerFactory/NAME ^"[Ljava.lang.String;" (into-array String []))
            (.addTokenFilter ^String LowerCaseFilterFactory/NAME ^"[Ljava.lang.String;" (into-array String [])))))

(defn ^Analyzer ->per-field-analyzer
  "global analyzer to cover indexing and _nearly_ all queries (it can't help with overriding dynamic fields)"
  [_]
  (PerFieldAnalyzerWrapper. (->custom-analyzer nil) {field-xt-val-exact (KeywordAnalyzer.)}))

(defn build-custom-query [analyzer field-string v]
  (let [qp (doto (QueryParser. field-string analyzer) (.setAllowLeadingWildcard true))
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defn custom-text-search
  ([node db attr query]
   (custom-text-search node db attr query {}))
  ([node db attr query opts]
   (with-open [s (l/search node query)]
     (->> (iterator-seq s)
          (map (fn doc->rel [[^Document doc score]]
                 [(.get ^Document doc l/field-xt-val) score]))
          (mapcat (fn resolve-atemporal-av [[v s]]
                    (c/q db {:find '[e v s]
                             :in '[v s]
                             :where [['e attr 'v]]}
                         v s)))
          (into [])))))

(defn custom-wildcard-text-search
  ([node db query]
   (custom-wildcard-text-search node db query {}))
  ([node db query opts]
   (with-open [s (l/search node
                           query
                           opts)]
     (->> (iterator-seq s)
          (map (fn doc->rel [[^Document doc score]]
                 [(.get ^Document doc l/field-xt-attr) (.get ^Document doc l/field-xt-val) score]))
          (mapcat (fn resolve-atemporal-av [[a v s]]
                    (let [a (keyword a)]
                      (c/q db {:find '[e a v s]
                               :in '[a v s]
                               :where [['e a 'v]]}
                           a v s))))
          (into [])))))

(t/deftest test-custom-index-and-analyzer
  (with-open [node (c/start-node {:crux.lucene/lucene-store {:analyzer 'crux.lucene.extension-test/->per-field-analyzer
                                                             :indexer 'crux.lucene.extension-test/->custom-eav-indexer}})]
    (submit+await-tx node [[:xt/put {:xt/id 0, :description "Some Entity"}]
                           [:xt/put {:xt/id 1, :description "Another entity"}]])

    (let [db (c/db node)]
      ;; text-search + leading-wildcard
      (t/is (= [[0 "Some Entity" 2.0]
                [1 "Another entity" 1.0]]
               (let [a :description]
                 (custom-text-search node db a (build-custom-query (->per-field-analyzer nil) (l/keyword->k a) "so* *En*")))))

      ;; case-sensitive text-search + leading-wildcard
      (t/is (= [[0 "Some Entity" 1.0]]
               (let [a :description]
                 ;; as per the docstring above, using KeywordAnalyzer here because PerFieldAnalyzerWrapper only helps with hardcoded fields
                 (custom-text-search node db a (build-custom-query (KeywordAnalyzer.) (keyword->kcs a) "*En*")))))

      ;; wildcard-text-search + leading wildcard
      (t/is (= [[0 :description "Some Entity" 2.0]
                [1 :description "Another entity" 1.0]]
               (custom-wildcard-text-search node db (build-custom-query (->per-field-analyzer nil) l/field-xt-val "*So* *En*"))))

      ;; case-sensitive wildcard-text-search + leading wildcard
      (t/is (= [[0 :description "Some Entity" 1.0]]
               (custom-wildcard-text-search node db (build-custom-query (->per-field-analyzer nil) field-xt-val-exact "*En*")))))))

;;; how-to example

(defrecord ExampleEavIndexer []
  l/LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [{e :xt/id, :as crux-doc} (vals docs)
            [a v] (->> (dissoc crux-doc :xt/id)
                       (mapcat (fn [[a v]]
                                 (for [v (cc/vectorize-value v)
                                       :when (string? v)]
                                   [a v]))))
            :when (contains? #{:product/title :product/description} a) ;; example - don't index all attributes
            :let [id-str (l/->hash-str (l/->DocumentId e a v))
                  doc (doto (Document.)
                        ;; To search for triples by e-a-v for deduping
                        (.add (StringField. l/field-xt-id, id-str, Field$Store/NO))
                        ;; The actual term, which will be tokenized
                        (.add (TextField. (l/keyword->k a), v, Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (TextField. l/field-xt-val, v, Field$Store/YES))
                        ;; Used for eviction
                        (.add (StringField. l/field-xt-eid, (l/->hash-str e), Field$Store/NO))
                        ;; Used for wildcard searches
                        (.add (StringField. l/field-xt-attr, (l/keyword->k a), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. l/field-xt-id id-str) doc)))

  (evict! [_ index-writer eids]
    (let [qs (for [eid eids]
               (TermQuery. (Term. l/field-xt-eid (l/->hash-str eid))))]
      (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn ->example-eav-indexer [_]
  (ExampleEavIndexer.))

(defn ^Analyzer ->example-analyzer
  "case-insensitive analyzer"
  [_]
  (.build (doto (CustomAnalyzer/builder)
            (.withTokenizer ^String StandardTokenizerFactory/NAME ^"[Ljava.lang.String;" (into-array String []))
            (.addTokenFilter ^String LowerCaseFilterFactory/NAME ^"[Ljava.lang.String;" (into-array String [])))))

(t/deftest test-example-use-case
  (with-open [node (c/start-node {:crux.lucene/lucene-store {:analyzer 'crux.lucene.extension-test/->example-analyzer
                                                             :indexer 'crux.lucene.extension-test/->example-eav-indexer}})]
    (submit+await-tx node [[:xt/put {:xt/id "SKU-32934"
                                     :product/title "Brown Summer Hat"
                                     :product/description "This large and comfortable woven hat will keep you cool in the summer"}]
                           [:xt/put {:xt/id "SKU-93921"
                                     :product/title "Yellow Raincoat"
                                     :product/description "Light and bright - a yellow raincoat to keep you dry all year round"}]
                           [:xt/put {:xt/id "SKU-13892"
                                     :product/title "Large Umbrella"
                                     :product/description "Don't let rain get in the way of your day!"}]])
    (let [db (c/db node)]
      ;; text-search + leading-wildcard
      (t/is (= [["SKU-32934" "Brown Summer Hat" 1.0]]
               (let [a :product/title]
                 (custom-text-search node db a (build-custom-query (->per-field-analyzer nil) (l/keyword->k a) "*hat*")))))

      ;; wildcard-text-search + leading wildcard + boosted :product/title
      (t/is (= [["SKU-93921" :product/title "Yellow Raincoat" 10.0]
                ["SKU-13892"
                 :product/description
                 "Don't let rain get in the way of your day!"
                 1.0]
                ["SKU-93921"
                 :product/description
                 "Light and bright - a yellow raincoat to keep you dry all year round"
                 1.0]]
               (->> (custom-wildcard-text-search node
                                                 db
                                                 (FunctionScoreQuery/boostByQuery
                                                  (build-custom-query (->per-field-analyzer nil)
                                                                      l/field-xt-val
                                                                      "*rain*")
                                                  (build-custom-query (->per-field-analyzer nil)
                                                                      l/field-xt-attr
                                                                      (-> :product/title
                                                                          l/keyword->k
                                                                          QueryParser/escape))
                                                  10.0))
                    (sort-by (juxt (comp - last) first))))))))
