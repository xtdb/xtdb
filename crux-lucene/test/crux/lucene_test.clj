1(ns crux.lucene-test
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api* submit+await-tx with-tmp-dir]])
  (:import org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.index IndexWriterConfig IndexWriter DirectoryReader]
           [org.apache.lucene.store FSDirectory]
           [org.apache.lucene.document Document Field TextField]
           [org.apache.lucene.search IndexSearcher ScoreDoc]
           org.apache.lucene.queryparser.classic.QueryParser))

(t/use-fixtures :each fix/with-standalone-topology fix/with-kv-dir fix/with-node)

(t/deftest test-can-search-string-wildcard-2
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

  (with-tmp-dir "kv-store" [db-dir]
    (with-open [d (FSDirectory/open (.toPath ^java.io.File db-dir))]
      (let [analyzer (StandardAnalyzer.)
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
            (t/is (= 1 (count (.-scoreDocs (.search isearcher q 10)))))))))))

;; See https://github.com/juxt/crux-rnd/blob/master/src/crux/datalog/lucene.clj
;; https://www.toptal.com/database/full-text-search-of-dialogues-with-apache-lucene
;; https://lucene.apache.org/core/8_6_0/core/index.html
