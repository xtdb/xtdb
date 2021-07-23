(ns crux.lucene.extension-test
  (:require  [clojure.test :as t]
             [clojure.spec.alpha :as s]
             [crux.api :as c]
             [crux.checkpoint :as cp]
             [crux.db :as db]
             [crux.fixtures :as fix :refer [*api* submit+await-tx]]
             [crux.fixtures.lucene :as lf]
             [crux.lucene :as l]))

(require 'crux.lucene.multi-field :reload) ; for dev, due to changing protocols

(t/deftest test-multiple-lucene-stores
  (with-open [node (c/start-node {:ave {:crux/module 'crux.lucene/->lucene-store
                                        :analyzer 'crux.lucene/->analyzer
                                        :indexer 'crux.lucene/->indexer}
                                  :multi {:crux/module 'crux.lucene/->lucene-store
                                          :analyzer 'crux.lucene/->analyzer
                                          :indexer 'crux.lucene.multi-field/->indexer}})]
    (submit+await-tx node [[:crux.tx/put {:crux.db/id :ivan
                                          :firstname "Fred"
                                          :surname "Smith"}]])
    (with-open [db (c/open-db node)]
      (t/is (seq (c/q db '{:find [?e]
                           :where [[(text-search :firstname "Fre*" {:lucene-store-k :ave}) [[?e]]]
                                   [(lucene-text-search "firstname:%s AND surname:%s" "Fred" "Smith" {:lucene-store-k :multi}) [[?e]]]]}))))))
