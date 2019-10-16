(ns crux-microbench.cached-entities-index-in-vitro
  "Microbench for cached entities index in vacuum"
  (:require [clojure.test :as t]
          [crux.fixtures :as f]
          [crux.api :as api]
          [crux.index :as idx]
          [crux.lru :as lru]
          [crux.codec :as c]
          [crux.db :as db]
          [crux.fixtures :as f]
          [crux-microbench.ticker-data-gen :as data-gen]
          [crux.fixtures.api :refer [*api*]]
          [crux.fixtures.standalone :as fs])
  (:import (java.util Date)))

(defn- -microbench-cached-index []
  (f/transact! *api* data-gen/currencies)
  (let [db (api/db *api*)
        d (Date.)]
    (println "cache hit gains")
    (with-open [snapshot (api/new-snapshot db)]
      (let [idx-raw (idx/new-entity-as-of-index snapshot d d)
            idx-in-cache (lru/new-cached-index idx-raw 100)
            id-buf (c/->id-buffer :currency.id/eur)

            seeked (db/seek-values idx-raw id-buf)
            seeked-2 (db/seek-values idx-in-cache id-buf)]

        (println "seeked" :currency.id/eur "found" seeked)
        (println "seeked-2" :currency.id/eur "found" seeked-2)

        (println "microbench for raw idx")
        (time
          (dotimes [_ 100000]
            (db/seek-values idx-raw id-buf)))

        (println "microbench for a cached idx")
        (time
          (dotimes [_ 100000]
            (db/seek-values idx-in-cache id-buf)))

        (t/is seeked)
        (t/is seeked-2))))

  (let [db (api/db *api*)
        tickers (data-gen/gen-tickers 1000)
        _ (f/transact! *api* tickers)
        d (Date.)]
    (println "cache miss overhead")
    (with-open [snapshot (api/new-snapshot db)]
      (let [idx-raw (idx/new-entity-as-of-index snapshot d d)
            idx-in-cache (lru/new-cached-index idx-raw 100)
            stock-id-buffs (mapv (comp c/->id-buffer :crux.db/id) tickers)
            id-buf (c/->id-buffer :currency.id/eur)

            seeked (db/seek-values idx-raw id-buf)
            seeked-2 (db/seek-values idx-in-cache id-buf)]

        (println "seeked" :currency.id/eur "found" seeked)
        (println "seeked-2" :currency.id/eur "found" seeked-2)

        (println "microbench for raw idx")
        (time
          (doseq [id-buf stock-id-buffs]
            (db/seek-values idx-raw id-buf)))

        (println "microbench for a cached idx")
        (time
          (doseq [id-buf stock-id-buffs]
            (db/seek-values idx-in-cache id-buf)))

        (t/is seeked)
        (t/is seeked-2)
        nil))))

(defn ^:microbench microbench-cached-index []
  (fs/with-standalone-node -microbench-cached-index))

(comment
  (microbench-cached-index))