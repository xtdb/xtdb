(ns crux.lucene-test
  (:require [clojure.test :as t]
            [crux.lucene]
            [crux.node :as n]
            [crux.fixtures :as fix :refer [*api* submit+await-tx with-tmp-dir]])
  (:import org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.index IndexWriterConfig IndexWriter DirectoryReader]
           [org.apache.lucene.store FSDirectory Directory]
           [org.apache.lucene.document Document Field TextField]
           [org.apache.lucene.search IndexSearcher ScoreDoc]
           org.apache.lucene.queryparser.classic.QueryParser))

(defn with-lucene-module [f]
  (with-tmp-dir "lucene" [db-dir]
    (fix/with-opts (-> fix/*opts*
                       (update ::n/topology conj crux.lucene/module)
                       (assoc :crux.lucene/db-dir db-dir))
      f)))

(t/use-fixtures :each fix/with-standalone-topology with-lucene-module fix/with-kv-dir fix/with-node)

(t/deftest test-can-search-string-wildcard-2
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

  (let [d ^Directory (:directory (:crux.lucene/node (:crux.node/topology (meta *api*))))
        analyzer (StandardAnalyzer.)
        iwc (IndexWriterConfig. analyzer)]

    ;; Index
    (with-open [iw (IndexWriter. d, iwc)]
      (let [doc (doto (Document.)
                  (.add (Field. "FooField", "bar", TextField/TYPE_STORED)))]
        (.addDocument iw doc)))

    ;; Search
    (with-open [dr (DirectoryReader/open d)]
      (let [isearcher (IndexSearcher. dr)
            qp (QueryParser. "FooField" analyzer)
            q (.parse qp "bar")]
        (t/is (= 1 (count (.-scoreDocs (.search isearcher q 10)))))))))

;; See https://github.com/juxt/crux-rnd/blob/master/src/crux/datalog/lucene.clj
;; https://www.toptal.com/database/full-text-search-of-dialogues-with-apache-lucene
;; https://lucene.apache.org/core/8_6_0/core/index.html
