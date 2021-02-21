(ns crux.lucene.egeria-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]))

(t/use-fixtures :each (lf/with-lucene-opts {:analyzer 'crux.lucene.egeria/->analyzer}) fix/with-node)

(t/deftest test-egeria-queries
  (submit+await-tx [[:crux.tx/put {:crux.db/id :fred
                                   :firstname "Fred"
                                   :surname "SmItH sMiThSON"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(egeria-text-search "*mith*on") [[?e]]]]})))
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(egeria-text-search "*h s*") [[?e]]]]})))
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(egeria-text-search "*o*") [[?e]]]]})))
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(egeria-text-search "*n") [[?e]]]]})))
    (t/is (not (seq (c/q db {:find '[?e]
                             :where '[[(egeria-text-search "*x*") [[?e]]]]}))))))
