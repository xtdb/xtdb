(ns crux.lucene.multi-field-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l]
            [crux.lucene.multi-field :as lmf]))

(t/use-fixtures :each (lf/with-lucene-opts {:indexer 'crux.lucene.multi-field/->indexer}) fix/with-node)

(t/deftest test-multi-field-lucene-queries
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "firstname: Fred") [[?e]]]]})))
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "firstname:James OR surname:smith") [[?e]]]]})))
    (t/is (not (seq (c/q db {:find '[?e]
                             :where '[[(lucene-text-search "firstname:James OR surname:preston") [[?e]]]]}))))))

(t/deftest test-bindings
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Fred"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[?e :firstname ?firstname]
                                 [(lucene-text-search "surname: %s" ?firstname) [[?e]]]]})))))

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
                             (with-open [search-results (lf/search lmf/build-lucene-text-query ["name: Smith"])]
                               (boolean (seq (iterator-seq search-results))))))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Smith"}]])

    (assert (in-crux?))
    (assert (in-lucene-store?))

    (submit+await-tx [[:crux.tx/evict :ivan]])

    (t/is (not (in-crux?)))
    (t/is (not (in-lucene-store?)))))

;; todo test error handling, malformed query
