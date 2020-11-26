(ns crux.lucene-multi-field-test
    (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l]
            [crux.rocksdb :as rocks]))

(defn- index-docs! [document-store lucene-store docs]
  (with-open [index-writer (index-writer lucene-store)]
    (doseq [d docs t (crux-doc->triples d)]
      (.updateDocument index-writer (triple->term t) (triple->doc t)))))

(t/use-fixtures :each (lf/with-lucene-multi-docs-module index-docs!) fix/with-node)

;; We're looking for a minimal API surface area
;; how do we hook into ingest?

;; via tx-fns?
;; via index-docs, called per tx-fn (we see a doc, we grab the other ones)

;; configuration?!
;; would be nice if there was a way to override config

(t/deftest test-multi-val-docs

  ;;

  )

(t/deftest test-keyword-ids
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e ?v]
                        :where '[[(text-search :name "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

;; todo delete doc deletion / tombstones?
;; temporal docs (are they visible)
;; must be part of tx, decorator for submit-tx / tx-fn?
