(ns crux.lucene.multi-field-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l])
  (:import org.apache.lucene.queryparser.classic.ParseException))

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
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db '{:find [?e]
                         :in [?surname]
                         :where [[(lucene-text-search "surname: %s" ?surname) [[?e]]]]}
                    "Smith")))
    (t/is (seq (c/q db '{:find [?e]
                         :in [?surname ?firstname]
                         :where [[(lucene-text-search "surname: %s AND firstname: %s" ?surname ?firstname) [[?e]]]]}
                    "Smith" "Fred")))))

(t/deftest test-namespaced-keywords
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :person/surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    ;; QueryParser/escape also works
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(lucene-text-search "person\\/surname: Smith") [[?e]]]
                                 [?e :crux.db/id]]})))))

(t/deftest test-evict
  (let [in-crux? (fn []
                   (with-open [db (c/open-db *api*)]
                     (boolean (seq (c/q db {:find '[?e]
                                            :where '[[(lucene-text-search "name: Smith") [[?e]]]
                                                     [?e :crux.db/id]]})))))
        in-lucene-store? (fn []
                           (with-open [search-results (l/search *api* "name: Smith")]
                             (boolean (seq (iterator-seq search-results)))))]

    (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Smith"}]])

    (assert (in-crux?))
    (assert (in-lucene-store?))

    (submit+await-tx [[:crux.tx/evict :ivan]])

    (t/is (not (in-crux?)))
    (t/is (not (in-lucene-store?)))))

(t/deftest test-malformed-query
  (t/is (thrown-with-msg? ParseException #"Cannot parse"
                          (c/q (c/db *api*) {:find '[?e]
                                             :where '[[(lucene-text-search "+12!") [[?e]]]
                                                      [?e :crux.db/id]]}))))

(t/deftest test-use-in-argument
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db '{:find [?e]
                         :in [?s]
                         :where [[(lucene-text-search ?s) [[?e]]]]}
                    "firstname: Fred")))
    (t/is (not (seq (c/q db '{:find [?e]
                              :in [?s]
                              :where [[(lucene-text-search ?s) [[?e]]]]}
                         "firstname Fred"))))
    (t/is (seq (c/q db '{:find [?e]
                         :in [?s]
                         :where [[(lucene-text-search ?s) [[?e]]]]}
                    "firstname:James OR surname:smith")))
    (t/is (thrown-with-msg? IllegalArgumentException #"lucene-text-search query must be String"
                            (c/q db '{:find  [?v]
                                      :in    [input]
                                      :where [[(lucene-text-search input) [[?e ?v]]]]}
                                 1)))))

(defn build-lucene-multi-field-or-string [kw-fields term-string]
  (apply str
         (interpose " OR " (for [field kw-fields]
                             (str (subs (str field) 1) ":" term-string)))))

(t/deftest test-construct-or-fields-dynamically
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db '{:find [?e]
                         :in [?s]
                         :where [[(lucene-text-search ?s) [[?e]]]]}
                    (build-lucene-multi-field-or-string [:firstname :surname] "Fre*"))))))
