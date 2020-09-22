(ns crux.lucene-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx with-tmp-dir]]
            [crux.lucene :as l])
  (:import org.apache.lucene.document.Document))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts {::l/node {:db-dir (.toPath ^java.io.File db-dir)}
                    :crux/indexer {:crux/module 'crux.lucene/->indexer
                                   :indexer 'crux.kv.indexer/->kv-indexer}}
      f)))

(t/use-fixtures :each with-lucene-module fix/with-node)

(t/deftest test-can-search-string
  (let [doc {:crux.db/id :ivan :name "Ivan"}]
    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

    (t/testing "using Lucene directly"
      (with-open [search-results ^crux.api.ICursor (l/search (:crux.lucene/node @(:!system *api*)) "name" "Ivan")]
        (let [docs (iterator-seq search-results)]
          (t/is (= 1 (count docs)))
          (t/is (= "Ivan" (.get ^Document (ffirst docs) "name"))))))

    (t/testing "using predicate function"
      (with-open [db (c/open-db *api*)]
        (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                     :where '[[(crux.lucene/full-text node db :name "Ivan") [[?e]]]
                                              [?e :crux.db/id]]
                                     :args [{:db db :node (:crux.lucene/node @(:!system *api*))}]})))))

    (t/testing "using in-built function"
      (with-open [db (c/open-db *api*)]
        (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                     :where '[[(text-search :name "Ivan") [[?e]]]
                                              [?e :crux.db/id]]})))

        (t/testing "bad spec"
          (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                                  (c/q db {:find '[?e]
                                           :where '[[(text-search "Ivan") [[?e]]]
                                                    [?e :crux.db/id]]}))))

        (t/testing "fuzzy"
          (t/is (= #{[:ivan]} (c/q db {:find '[?e]
                                       :where '[[(text-search :name "Iv*") [[?e]]]
                                                [?e :crux.db/id]]}))))))

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
      (submit+await-tx [[:crux.tx/evict :ivan]])
      (with-open [db (c/open-db *api*)]
        (t/is (empty? (c/q db {:find '[?e]
                               :where
                               '[[(text-search :name "Ivan") [[?e]]]
                                 [?e :crux.db/id]]}))))
      (with-open [search-results ^crux.api.ICursor (l/search (:crux.lucene/node @(:!system *api*)) "name" "Ivan")]
        (t/is (empty? (iterator-seq search-results)))))

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

#_(t/deftest test-scoring-shouldnt-be-impacted-by-non-matched-past-docs
  (submit+await-tx [[:crux.tx/put {:crux.db/id :real-ivan :name "Ivan Bob"}]])

  (let [q {:find '[?v ?score]
           :where '[[(text-search :name "Ivan") [[?e ?v ?score]]]
                    [?e :crux.db/id]]}

        prior-score (with-open [db (c/open-db *api*)]
                      (c/q db q))]

    (doseq [n (range 10)]
      (submit+await-tx [[:crux.tx/put {:crux.db/id (str "id-" n) :name "NO MATCH"}]])
      (submit+await-tx [[:crux.tx/delete (str "id-" n)]]))

    ;; The past data has made the doc we're looking for more
    ;; rare/unique, thus it will get a higher score:

    (with-open [db (c/open-db *api*)]
      (t/is (= prior-score (c/q db q))))))

#_(t/deftest test-scoring-shouldnt-be-impacted-by-matched-past-docs
  (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan" :name "Ivan Bob Bob"}]])

  (let [q {:find '[?e ?v ?s]
           :where '[[(text-search :name "Ivan") [[?e ?v ?s]]]
                    [?e :crux.db/id]]}
        prior-score (with-open [db (c/open-db *api*)]
                      (c/q db q))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id "ivan1" :name "Ivan"}]])
    (submit+await-tx [[:crux.tx/delete "ivan1"]])

    ;; The more exacting string in the past is distorting the present:
    ;; This is still due to an idf effect (not as pronounced as uniqueness / rare)
    ;; and more due to the avgdl changing (average length of field)

    (with-open [db (c/open-db *api*)]
      (t/is (= prior-score (c/q db q))))))

(t/deftest test-keyword-ids-are-breaking
  (submit+await-tx [[:crux.tx/put {:crux.db/id :real-ivan-2 :name "Ivan Bob"}]]))
