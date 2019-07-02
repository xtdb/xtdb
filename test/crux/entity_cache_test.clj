(ns crux.entity-cache-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.bench :as bench-tools]
            [crux.api :as api]
            [crux.index :as idx]
            [crux.lru :as lru]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures.bootstrap :as fb]
            [crux.fixtures.kafka :as fk])
  (:import (java.util Date)))

(defn avg [nums]
  (/ (reduce + nums) (count nums)))

(def run-entity-cache-tests? (Boolean/parseBoolean (System/getenv "CRUX_TEST_ENTITY_CACHE")))
;(def run-entity-cache-tests? true)

(def currencies
  (mapv #(assoc % :crux.db/id (:currency/id %))
        [{:currency/id :currency.id/eur
          :currency/ratio-to-euro 1
          :currency/name "Euro"}
         {:currency/id :currency.id/usd
          :currency/ratio-to-euro 0.9
          :currency/name "USD"}
         {:currency/id :currency.id/chf
          :currency/ratio-to-euro 0.9
          :currency/name "Swiss Franc"}
         {:currency/id :currency.id/gbp
          :currency/ratio-to-euro 1.1
          :currency/name "British Pound"}
         {:currency/id :currency.id/rub
          :currency/ratio-to-euro 0.05
          :currency/name "Russian Rouble"}
         {:currency/id :currency.id/cny
          :currency/ratio-to-euro 0.1
          :currency/name "Chinese Yuan"}
         {:currency/id :currency.id/jpy
          :currency/ratio-to-euro 0.01
          :currency/name "Japanese Yen"}]))

(defn gen-stock [i]
  (let [id (keyword "stock.id" (str "company-" i))]
    {:stock/id id
     :crux.db/id id
     :stock/price (rand-int 1000)
     :stock/currency-id (:currency/id (rand-nth currencies))
     :stock/backup-currency-id (:currency/id (rand-nth currencies))}))

(defn alter-stock [stock]
  (assoc stock :stock/price (rand-int 1000)))

(defn alter-currency [currency]
  (update currency :currency/ratio-to-euro + (* 0.1 (rand))))

(defn alter-stocks [pack]
  (map alter-stock pack))

(def query-size 100000)
(def stocks-count 100000)
(def history-days 1)
(def sample-size 20)

(def -stocks (atom nil))

(def query-1
  '[:find stock-id currency-id
    :where
    [currency-id :currency/name currency-name]
    [stock-id :stock/currency-id currency-id]])

(def query-3
  '[:find stock-id currency-name backup-currency-name
    :where
    [currency-id :currency/name currency-name]
    [backup-currency-id :currency/name backup-currency-name]
    [stock-id :stock/currency-id currency-id]
    [stock-id :stock/backup-currency-id backup-currency-id]])

(defn with-stocks-data [f]
  (when run-entity-cache-tests?
    (api/submit-tx fb/*api* (f/maps->tx-ops currencies))
    (println "stocks count" stocks-count)
    (let [ctr (atom 0)
          stocks (map gen-stock (range stocks-count))
          partitions-total (/ stocks-count 1000)]
      (reset! -stocks stocks)
      (doseq [stocks-batch (partition-all 1000 stocks)]
        (swap! ctr inc)
        (println "partition " @ctr "/" partitions-total)
        (api/submit-tx fb/*api* (f/maps->tx-ops stocks-batch))))
    (api/sync fb/*api* (java.time.Duration/ofMinutes 20)))
  (f))

(defn with-stocks-history-data [f]
  (when run-entity-cache-tests?
    (println "stocks count" stocks-count)
    (let [base-stocks (map gen-stock (range stocks-count))
          base-date #inst "2019"
          partitions-total (/ stocks-count 1000)]
      (reset! -stocks base-stocks)
      (doseq [i (range history-days)
              :let [date (Date. ^long (+ (.getTime base-date) (* 1000 60 60 24 i)))
                    ctr (atom 0)
                    stocks-batch (alter-stocks base-stocks)
                    t-currencies (map alter-currency currencies)]]
        (doseq [stocks-batch (partition-all 1000 stocks-batch)]
          (swap! ctr inc)
          (println "day " i "\t" "partition " @ctr "/" partitions-total)
          (api/submit-tx fb/*api* (f/maps->tx-ops stocks-batch)))
        (api/submit-tx fb/*api* (f/maps->tx-ops t-currencies date)))
      (println
        "Sync takes: "
        (crux.bench/duration-millis
          (api/sync fb/*api* (java.time.Duration/ofMinutes 20))))))
  (f))

(t/use-fixtures :once
                fk/with-embedded-kafka-cluster
                fk/with-kafka-client
                fb/with-cluster-node
                with-stocks-history-data)

(defn not-really-benchmarking [query db n]
  (for [_ (range n)]
    (crux.bench/duration-millis (api/q db query))))

(t/deftest test-cached-index
  (when run-entity-cache-tests?
    (t/testing "cache hit gains"
      (let [db (api/db fb/*api*)
            d (Date.)]
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

            (t/is (some? seeked))
            (t/is (some? seeked-2))))))

    (t/testing "cache miss overhead"
      (let [db (api/db fb/*api*)
            d (Date.)]
        (with-open [snapshot (api/new-snapshot db)]
          (let [idx-raw (idx/new-entity-as-of-index snapshot d d)
                idx-in-cache (lru/new-cached-index idx-raw 100)
                stock-id-buffs (mapv (comp c/->id-buffer :crux.db/id) @-stocks)
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

            (t/is (some? seeked))
            (t/is (some? seeked-2))))))))

(defn- test-query [test-id query cache-on?]
  (binding [crux.query/*with-entities-cache?* cache-on?]
     (let [db (api/db fb/*api*)
           -dry (crux.bench/duration-millis (api/q db query))
           durations (not-really-benchmarking query db sample-size)
           res (api/q db query)]
       (println test-id :durations durations)
       (println test-id :avg (avg durations))
       #_(println "res-size" (count res))
       #_(println "rand-sample" (take 10 (random-sample 0.1 res)))
       (t/is true))))

(t/deftest test-cache-frequencies-2
  (when run-entity-cache-tests?
    (println "\n")
    (println :days history-days :stocks stocks-count)
    (println)
    (test-query :q1-entity-cache-off query-1 false)
    (test-query :q1-entity-cache-on query-1 true)
    (println "\n")
    (test-query :q3-entity-cache-off query-3 false)
    (test-query :q3-entity-cache-on query-3 true)
    (println "\n")))

;(t/run-tests 'crux.entity-cache-test)
