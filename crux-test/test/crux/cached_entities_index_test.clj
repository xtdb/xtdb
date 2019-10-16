(ns crux.cached-entities-index-test
  "Sanity check for cached entities index.
  For microbench see crux-dev/dev/crux-microbench/cached-entities-index-bench"
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.index :as idx]
            [crux.lru :as lru]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :as apif]))

(t/use-fixtures :each fs/with-standalone-node kvf/with-kv-dir apif/with-node)

(t/deftest test-cached-index
  (t/testing "cached index sanity"
    (f/transact! *api* [{:crux.db/id :currency.id/eur}])
    (let [db (api/db *api*)
          d (java.util.Date.)]
      (t/is (api/entity db :currency.id/eur))
      (with-open [snapshot (api/new-snapshot db)]
        (let [idx-raw (idx/new-entity-as-of-index snapshot d d)
              idx-in-cache (lru/new-cached-index idx-raw 100)
              id-buf (c/->id-buffer :currency.id/eur)
              seeked (db/seek-values idx-raw id-buf)
              seeked-2 (db/seek-values idx-in-cache id-buf)]
          (t/is (some? seeked))
          (t/is (some? seeked-2)))))))
