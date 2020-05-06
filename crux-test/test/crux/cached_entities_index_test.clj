(ns crux.cached-entities-index-test
  "Sanity check for cached entities index.
  For microbench see dev/crux-microbench/cached-entities-index-bench"
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.index :as idx]
            [crux.lru :as lru]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.kv :as kv]
            [crux.fixtures :as fix :refer [*api*]]))

(t/use-fixtures :each fix/with-standalone-topology fix/with-kv-dir fix/with-node)

(t/deftest test-cached-index
  (t/testing "cached index sanity"
    (fix/transact! *api* [{:crux.db/id :currency.id/eur}])
    (let [db (api/db *api*)
          d (java.util.Date.)]
      (t/is (api/entity db :currency.id/eur))
      (with-open [snapshot (api/new-snapshot db)
                  i (kv/new-iterator snapshot)]
        (let [idx-raw (idx/new-entity-as-of-index i d d)
              idx-in-cache (lru/new-cached-index idx-raw 100)
              id-buf (c/->id-buffer :currency.id/eur)
              seeked (db/seek-values idx-raw id-buf)
              seeked-2 (db/seek-values idx-in-cache id-buf)]
          (t/is (some? seeked))
          (t/is (some? seeked-2)))))))
