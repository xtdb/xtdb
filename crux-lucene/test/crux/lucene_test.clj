(ns crux.lucene-test
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.api :as c]
            [crux.checkpoint :as cp]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l]
            [crux.query :as q]
            [crux.rocksdb :as rocks])
  (:import org.apache.lucene.analysis.Analyzer
           org.apache.lucene.document.Document
           [org.apache.lucene.index DirectoryReader Term]
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder Query]
           org.apache.lucene.store.FSDirectory))

;; tests in this namespace depend on the `(defmethod q/pred-constraint 'lucene-text-search ...)`
(require 'crux.lucene.multi-field)

(t/use-fixtures :each (fix/with-opts {::l/lucene-store {}}) fix/with-node)

(t/deftest test-empty-database-returns-empty
  (t/is (= 0 (count (c/q (c/db *api*) {:find '[?e]
                                       :where '[[(text-search :name "Ivan") [[?e]]]]})))))

(t/deftest test-can-search-string
  (let [doc {:crux.db/id :ivan :name "Ivan"}]
    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

    (t/testing "using Lucene directly"
      (with-open [search-results (l/search *api* "Ivan" {:default-field "name"})]
        (let [docs (iterator-seq search-results)]
          (t/is (= 1 (count docs)))
          (t/is (= "Ivan" (.get ^Document (ffirst docs) "_crux_val"))))))

    (t/testing "using in-built function"
      (with-open [db (c/open-db *api*)]
        (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                     :where '[[(text-search :name "Ivan") [[?e]]]]})))

        (t/testing "bad spec"
          (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                                  (c/q db {:find '[?e]
                                           :where '[[(text-search "Wot" "Ivan") [[?e]]]]}))))

        (t/testing "fuzzy"
          (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                       :where '[[(text-search :name "Iv*") [[?e]]]]}))))))

    (t/testing "Subsequent tx/doc"
      (with-open [before-db (c/open-db *api*)]
        (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan2 :name "Ivbn"}]])
        (let [q {:find '[?e] :where '[[(text-search :name "Iv?n") [[?e]]] [?e :crux.db/id]]}]
          (t/is (= #{[:ivan]} (c/q before-db q)))
          (with-open [db (c/open-db *api*)]
            (t/is (= #{[:ivan] [:ivan2]} (c/q db q)))))))

    (t/testing "Modifying doc"
      (with-open [before-db (c/open-db *api*)]
        (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Derek"}]])
        (let [q {:find '[?e] :where '[[(text-search :name "Derek") [[?e]]] [?e :crux.db/id]]}]
          (t/is (not (seq (c/q before-db q))))
          (with-open [db (c/open-db *api*)]
            (t/is (= #{[:ivan]} (c/q db q)))))))

    (t/testing "Eviction"
      (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan2 :name "Derek"}]])
      (submit+await-tx [[:crux.tx/evict :ivan]])
      (with-open [db (c/open-db *api*)]
        (t/is (empty? (c/q db {:find '[?e]
                               :where
                               '[[(text-search :name "Ivan") [[?e]]]
                                 [?e :crux.db/id]]}))))
      (with-open [search-results (l/search *api* "Ivan" {:default-field "name"})]
        (t/is (empty? (iterator-seq search-results))))
      (with-open [search-results (l/search *api* "Derek" {:default-field "name"})]
        (t/is (seq (iterator-seq search-results)))))

    (t/testing "Scores"
      (submit+await-tx [[:crux.tx/put {:crux.db/id "test0" :name "ivon"}]])
      (submit+await-tx [[:crux.tx/put {:crux.db/id "test1" :name "ivan"}]])
      (submit+await-tx [[:crux.tx/put {:crux.db/id "test2" :name "testivantest"}]])
      (submit+await-tx [[:crux.tx/put {:crux.db/id "test3" :name "testing"}]])
      (submit+await-tx [[:crux.tx/put {:crux.db/id "test4" :name "ivanpost"}]])
      (with-open [db (c/open-db *api*)]
        (t/is (= #{["test1" "ivan" 1.0] ["test4" "ivanpost" 1.0]}
                 (c/q db {:find '[?e ?v ?score]
                          :where '[[(text-search :name "ivan*") [[?e ?v ?score]]]
                                   [?e :crux.db/id]]})))))

    (t/testing "cardinality many"
      (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :foo #{"atar" "abar" "nomatch"}}]])

      (with-open [db (c/open-db *api*)]
        (t/is (= #{[:ivan "atar"]}
                 (c/q db {:find '[?e ?v]
                          :where '[[(text-search :foo "atar") [[?e ?v]]]
                                   [?e :crux.db/id]]}))))

      (with-open [db (c/open-db *api*)]
        (t/is (= #{[:ivan "abar"]
                   [:ivan "atar"]}
                 (c/q db {:find '[?e ?v]
                          :where '[[(text-search :foo "a?ar") [[?e ?v]]]
                                   [?e :crux.db/id]]})))))))

(t/deftest test-can-search-string-across-attributes
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

  (with-open [db (c/open-db *api*)]
    (t/testing "dont specify A"
      (t/is (= #{[:ivan "Ivan" :name]}
               (c/q db {:find '[?e ?v ?a]
                        :where '[[(wildcard-text-search "Ivan") [[?e ?v ?a]]]
                                 [?e :crux.db/id]]}))))

    (t/testing "no match against a non-existant field"
      (t/is (= #{}
               (c/q db {:find '[?e ?v]
                        :where '[[(text-search :non-field "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :surname "Ivan"}]])

  (t/testing "can find multiple a/vs"
    (with-open [db (c/open-db *api*)]
      (t/is (= #{[:ivan "Ivan" :name]
                 [:ivan "Ivan" :surname]}
               (c/q db {:find '[?e ?v ?a]
                        :where '[[(wildcard-text-search "Ivan") [[?e ?v ?a _]]]
                                 [?e :crux.db/id]]}))))))

(t/deftest test-can-search-multiple-entities-with-same-av-pair
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]
                    [:crux.tx/put {:crux.db/id :ivan2 :name "Ivan"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (= #{[:ivan "Ivan"] [:ivan2 "Ivan"]}
             (c/q db {:find '[?e ?v]
                        :where '[[(text-search :name "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

;; https://github.com/juxt/crux/issues/1428

(t/deftest test-can-search-multiple-entities-with-same-av-pair-bug-1428
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan1 :name "Ivan"}]
                    [:crux.tx/put {:crux.db/id :ivan2 :name "Ivan"}]
                    [:crux.tx/put {:crux.db/id :ivan3 :name "Ivan1"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (= #{[:ivan1] [:ivan2] [:ivan3]}
             (c/q db {:find '[?e]
                      :where '[[(text-search :name "Iv*") [[?e]]]]})))
    (t/is (= #{[:ivan1] [:ivan2] [:ivan3]}
             (c/q db {:find '[?e]
                      :where '[[(wildcard-text-search "Iv*") [[?e]]]]})))))

;; Leaving to document when score is impacted by accumulated temporal data
#_(t/deftest test-scoring-shouldnt-be-impacted-by-non-matched-past-docs
  (submit+await-tx [[:crux.tx/put {:crux.db/id :real-ivan :name "Ivan Bob"}]])
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan-dave :name "Ivan Dave Ivan"}]])

  (let [q {:find '[?v ?score]
           :where '[[(text-search "Ivan" :name) [[?e ?v ?a ?score]]]
                    [?e :crux.db/id]]}

        prior-score (with-open [db (c/open-db *api*)]
                      (c/q db q))]

    (doseq [n (range 10)]
      (submit+await-tx [[:crux.tx/put {:crux.db/id (str "id-" n) :name "NO MATCH"}]])
      (submit+await-tx [[:crux.tx/delete (str "id-" n)]]))

    (with-open [db (c/open-db *api*)]
      (t/is (= prior-score (c/q db q))))))

;; Leaving to document when score is impacted by accumulated temporal data
#_(t/deftest test-scoring-shouldnt-be-impacted-by-matched-past-docs
  (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan" :name "Ivan Bob Bob"}]])

  (let [q {:find '[?e ?v ?s]
           :where '[[(text-search "Ivan" :name) [[?e ?v ?a ?s]]]
                    [?e :crux.db/id]]}
        prior-score (with-open [db (c/open-db *api*)]
                      (c/q db q))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan1" :name "Ivan"}]])
    (submit+await-tx [[:crux.tx/delete "ivan1"]])

    (with-open [db (c/open-db *api*)]
      (t/is (= prior-score (c/q db q))))))

(t/deftest test-structural-sharing
  (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan" :name "Ivan"}]])
  (let [q {:find '[?e ?v ?s]
           :where '[[(text-search :name "Ivan") [[?e ?v ?s]]]
                    [?e :crux.db/id]]}
        prior-score (with-open [db (c/open-db *api*)]
                      (c/q db q))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan" :name "Ivan"}]])
    (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan" :name "Ivan"}]])

    (t/is (= 1 (lf/doc-count)))

    (with-open [db (c/open-db *api*)]
      (t/is (= prior-score (c/q db q))))))

(t/deftest test-keyword-ids
  (submit+await-tx [[:crux.tx/put {:crux.db/id :real-ivan-2 :name "Ivan Bob"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e ?v]
                        :where '[[(text-search :name "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

(t/deftest test-namespaced-attributes
  (submit+await-tx [[:crux.tx/put {:crux.db/id :real-ivan-2 :myns/name "Ivan"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e ?v]
                        :where '[[(text-search :myns/name "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

(t/deftest test-past-fuzzy-results-excluded
  (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan0" :name "Ivan"}]])
  (submit+await-tx [[:crux.tx/delete "ivan0"]])
  (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan1" :name "Ivana"}]])

  (let [q {:find '[?e ?v ?s]
           :where '[[(text-search :name "Ivan*") [[?e ?v ?s]]]
                    [?e :crux.db/id]]}]
    (with-open [db (c/open-db *api*)]
      (t/is (= ["ivan1"] (map first (c/q db q)))))))

(t/deftest test-exclude-future-results
  (let [q {:find '[?e] :where '[[(text-search :name "Ivan") [[?e]]] [?e :crux.db/id]]}]
    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivanka"}]])
    (with-open [before-db (c/open-db *api*)]
      (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])
      (t/is (empty? (c/q before-db q))))))

(t/deftest test-ensure-lucene-store-keeps-last-tx
  (letfn [(latest-tx []
            (l/latest-completed-tx-id (-> @(:!system *api*)
                                          (get-in [:crux.lucene/lucene-store :index-writer]))))]
    (t/is (not (latest-tx)))
    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivank"}]])

    (t/is (latest-tx))))

(t/deftest test-lucene-catches-up-at-startup
  ;; Note, an edge case is if users change Lucene configuration
  ;; (i.e. indexing strategy) - this start-up check does not account
  ;; for this:

  (fix/with-tmp-dirs #{rocks-tmp-dir}
    (let [node-config {:crux/tx-log {:kv-store {:crux/module `rocks/->kv-store
                                                :db-dir (io/file rocks-tmp-dir "tx")}}
                       :crux/document-store {:kv-store {:crux/module `rocks/->kv-store
                                                        :db-dir (io/file rocks-tmp-dir "docs")}}
                       :crux/index-store {:kv-store {:crux/module `rocks/->kv-store
                                                     :db-dir (io/file rocks-tmp-dir "idx")}}}]
      (with-open [node (c/start-node node-config)]
        (submit+await-tx node [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]]))

      (fix/with-tmp-dirs #{lucene-tmp-dir}
        (with-open [node (c/start-node (-> node-config
                                           (assoc :crux.lucene/lucene-store {:db-dir lucene-tmp-dir})))]
          (t/is (seq (c/q (c/db node)
                          {:find '[?e ?v]
                           :where '[[(text-search :name "Ivan") [[?e ?v]]]
                                    [?e :crux.db/id]]}))))))))

(defn- with-lucene-rocks-node* [{:keys [node-dir index-dir lucene-dir]} f]
  (with-open [node (c/start-node (merge {:crux/document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                          :db-dir (io/file node-dir "documents")}}
                                         :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store,
                                                                  :db-dir (io/file node-dir "tx-log")}}}
                                        (when lucene-dir
                                          {:crux.lucene/lucene-store {:db-dir lucene-dir}})
                                        (when index-dir
                                          {:crux/index-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                         :db-dir index-dir}}})))]
    (binding [*api* node]
      (c/sync node)
      (f))))

(defmacro with-lucene-rocks-node [dir-keys & body]
  `(with-lucene-rocks-node* ~dir-keys (fn [] ~@body)))

(t/deftest test-lucene-cluster-node
  (t/testing "test restart with nil indexes"
    (fix/with-tmp-dirs #{node-dir}
      (fix/with-tmp-dirs #{lucene-dir}
        (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir}
          (submit+await-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])))

      (fix/with-tmp-dirs #{lucene-dir}
        (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir}
          (t/is (= (c/entity (c/db *api*) :ivan) {:crux.db/id :ivan :name "Ivan"})))))))

(t/deftest test-lucene-node-restart
  (t/testing "test restart with nil indexes"
    (fix/with-tmp-dirs #{node-dir lucene-dir}
      (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir}
        (submit+await-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]]))
      (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir}
        (t/is (= (c/entity (c/db *api*) :ivan) {:crux.db/id :ivan :name "Ivan"})))))

  (t/testing "restart with partial indices (> lucene-tx-id tx-id)"
    (fix/with-tmp-dirs #{node-dir lucene-dir index-dir}
      (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir :index-dir index-dir}
        (submit+await-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]]))
      (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir}
        (submit+await-tx *api* [[:crux.tx/put {:crux.db/id :fred :name "Fred"}]]))
      (with-lucene-rocks-node {:node-dir node-dir :lucene-dir lucene-dir :index-dir index-dir}
        (let [db (c/db *api*)]
          (t/is (= (c/entity db :fred) {:crux.db/id :fred :name "Fred"}))
          (t/is (= (c/entity db :ivan) {:crux.db/id :ivan :name "Ivan"}))
          (t/is (= #{[:fred]} (c/q db {:find '[?e]
                                       :where '[[(text-search :name "Fred") [[?e]]]
                                                [?e :crux.db/id]]})))
          (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                       :where '[[(text-search :name "Ivan") [[?e]]]
                                                [?e :crux.db/id]]}))))))))

(t/deftest test-id-can-be-key-1274
  (t/is (c/tx-committed? *api* (c/await-tx *api* (c/submit-tx *api* [[:crux.tx/put {:crux.db/id 512 :id "1"}]])))))

(defn ^Query build-or-query
  [^Analyzer analyzer, query-args]
  (let [[k, vs] query-args
        qp (QueryParser. (name k) analyzer)
        b  (BooleanQuery$Builder.)]
    (doseq [v vs]
      (.add b (.parse qp v) BooleanClause$Occur/SHOULD))
    (.build b)))

(defmethod q/pred-args-spec 'or-text-search [_]
  (s/cat :pred-fn #{'or-text-search} :args (s/spec (s/cat :attr keyword? :v (s/coll-of string?))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'or-text-search [_ pred-ctx]
  (let [resolver (partial l/resolve-search-results-a-v (second (:arg-bindings pred-ctx)))]
    (l/pred-constraint build-or-query resolver pred-ctx)))

(t/deftest test-or-text-search
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])
  (submit+await-tx [[:crux.tx/put {:crux.db/id :fred :name "Fred"}]])
  (submit+await-tx [[:crux.tx/put {:crux.db/id :matt :name "Matt"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                 :where '[[(or-text-search :name #{"Ivan"}) [[?e ?v]]]]})))

    (t/is (= #{[:ivan] [:fred]} (c/q db {:find '[?e]
                                         :where '[[(or-text-search :name #{"Ivan" "Fred"}) [[?e ?v]]]]})))))

(t/deftest results-not-limited-to-1000
  (submit+await-tx (for [n (range 1001)] [:crux.tx/put {:crux.db/id n, :description (str "Entity " n)}]))
  (with-open [db (c/open-db *api*)]
    (t/is (= 1001 (count (c/q db {:find '[?e]
                                  :where '[[(text-search :description "Entity*") [[?e]]]]}))))))

(t/deftest test-handle-incorrect-match-1456
  (submit+await-tx [[:crux.tx/put {:crux.db/id :test-id :name "1234"}]])
  (submit+await-tx [[:crux.tx/match :test-id {:crux.db/id :test-id}]
                    [:crux.tx/put {:crux.db/id :test-id :name "2345"}]])

  (t/is (= (:crux.tx/tx-id (db/latest-completed-tx (:crux/index-store @(:!system *api*))))
           (l/latest-completed-tx-id (-> @(:!system *api*)
                                         (get-in [::l/lucene-store :index-writer]))))))

(defn escape-lucene-string [s]
  ;; note this does not handle all cases if spaces are involved, e.g. a trailing " OR" will not be accepted by the QueryParser
  (org.apache.lucene.queryparser.classic.QueryParser/escape s))

(t/deftest test-use-in-argument
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"
                                   :escape-text "firstname:James"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db '{:find [?e]
                         :in [?s]
                         :where [[(wildcard-text-search ?s) [[?e]]]]}
                    "Fre*")))
    (t/is (seq (c/q db '{:find [?e]
                         :in [?s]
                         :where [[(crux.lucene-test/escape-lucene-string ?s) ?s*]
                                 [(wildcard-text-search ?s*) [[?e]]]]}
                    "firstname:James")))
    (t/is (thrown-with-msg? IllegalArgumentException #"Lucene text search values must be String"
                            (c/q db '{:find  [?v]
                                      :in    [input]
                                      :where [[(wildcard-text-search input) [[?e ?v]]]]}
                                 1)))))

(t/deftest test-checkpoint
  (fix/with-tmp-dirs #{lucene-dir cp-dir}
    (with-open [node (crux.api/start-node {::l/lucene-store {:db-dir lucene-dir}})]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :foo, :foo "foo"}]])

      (let [index-writer (-> @(:!system node)
                             (get-in [::l/lucene-store :index-writer]))
            src (#'l/checkpoint-src index-writer)]
        (cp/save-checkpoint src cp-dir)))

    (with-open [dir (FSDirectory/open (.toPath cp-dir))
                rdr (DirectoryReader/open dir)]
      (t/is (= 1 (.numDocs rdr))))))

(comment
  (do
    (import '[ch.qos.logback.classic Level Logger]
            'org.slf4j.LoggerFactory)
    (.setLevel ^Logger (LoggerFactory/getLogger "crux.lucene") (Level/valueOf "INFO"))))
