(ns crux.query-cache-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.bench :as bench-tools]
            [crux.api :as api]
            [crux.index :as idx]
            [crux.lru :as lru]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures.bootstrap :as fb])
  (:import (java.util Date)))

(def currencies
  (mapv #(assoc % :crux.db/id (:currency/id %))
        [{:currency/id :currency.id/eur
          :currency/name "Euro"}
         {:currency/id :currency.id/usd
          :currency/name "USD"}
         {:currency/id :currency.id/chf
          :currency/name "Swiss Franc"}
         {:currency/id :currency.id/gbp
          :currency/name "British Pound"}
         {:currency/id :currency.id/rub
          :currency/name "Russian Rouble"}
         {:currency/id :currency.id/cny
          :currency/name "Chinese Yuan"}
         {:currency/id :currency.id/jpy
          :currency/name "Japanese Yen"}]))

(def currencies-by-id
  (reduce (fn [m v] (assoc m (:crux.db/id v) v)) {} currencies))

(def id-buffer->id
  (let [ks (keys currencies-by-id)]
    (zipmap (map c/->id-buffer ks) ks)))

(defn gen-stock [i]
  (let [id (keyword "stock.id" (str "company-" i))]
    {:stock/id id
     :crux.db/id id
     :stock/price (rand-int 1000)
     :stock/currency-id (:currency/id (rand-nth currencies))
     :stock/backup-currency-id (:currency/id (rand-nth currencies))}))

(def stocks-count 1000)

(defn with-stocks-data [f]
  (api/submit-tx fb/*api* (f/maps->tx-ops currencies))
  (println "stocks count" stocks-count)
  (let [ctr (atom 0)
        partitions-total (/ stocks-count 1000)]
    (doseq [stocks-batch (partition-all 1000 (map gen-stock (range stocks-count)))]
      (swap! ctr inc)
      (println "partition " @ctr "/" partitions-total)
      (api/submit-tx fb/*api* (f/maps->tx-ops stocks-batch))))
  (api/sync fb/*api* (java.time.Duration/ofMinutes 20))
  (f))

(t/use-fixtures :once
                fb/with-each-api-implementation
                with-stocks-data)

(comment

  (def system
    (crux.api/start-standalone-system
      {:kv-backend "crux.kv.memdb.MemKv"
       :db-dir     "data/db-dir-1"}))

  (binding [f/*api* system]
    (with-stocks-data println))

  stocks

  (api/q (api/db system) query))

(def query
  '[:find stock-id currency-id
    :where
    [currency-id :currency/name currency-name]
    [stock-id :stock/currency-id currency-id]])

(def query-2
  '[:find stock-id currency-id
    :where
    [currency-id :currency/name "Euro"]
    [stock-id :stock/currency-id currency-id]])

(def query-3
  '[:find stock-id currency-name
    :where
    [currency-id :currency/name currency-name]
    [stock-id :stock/currency-id currency-id]
    (or [stock-id :stock/backup-currency-id "USD"]
        [stock-id :stock/backup-currency-id "Euro"])])

(def sample-size 10)

(defn not-really-benchmarking [db n]
  (for [_ (range n)]
    (crux.bench/duration-millis (api/q db query))))

; (not-really-benchmarking (crux.api ))


(t/deftest test-cached-index
  (let [db (api/db f/*api*)
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



(t/deftest test-stocks-query
  (println "running a stocks query with join to currencies")

  (binding [crux.query/*with-entities-cache?* false]
    (let [db (api/db fb/*api*)
          -dry (crux.bench/duration-millis (api/q db query))
          durations (not-really-benchmarking db sample-size)]
      (println "without cache durations in millis are" durations)
      (println "avg" (/ (apply + durations) sample-size))))

  (binding [crux.query/*with-entities-cache?* true]
    (let [db (api/db fb/*api*)
          -dry (crux.bench/duration-millis (api/q db query))
          durations (not-really-benchmarking db sample-size)]
      (println "with cache durations in millis are" durations)
      (println "avg" (/ (apply + durations) sample-size))))

  (t/is stocks-count (count (api/q (api/db fb/*api*) query))))

(t/deftest test-cache-frequencies
  (println "running a stocks query with join to currencies")

  (binding [crux.query/*with-entities-cache?* true]
    (let [db (api/db fb/*api*)
          -dry (crux.bench/duration-millis (api/q db query-3))
          durations (not-really-benchmarking db sample-size)]
     ;(println "with cache durations in millis are" durations)
     ;(println "avg" (/ (apply + durations) sample-size))

         #_(println "cache-size" (count (:index-cache crux.query/-cached)))
      #_(clojure.pprint/pprint (:index-cache crux.query/-cached))

     ;(println "cache hit frequencies")
      (let [freqs-src (frequencies @crux.lru/called-keys)
            total-entities-in-test (+ stocks-count (count currencies))
            accessed-keys-count (count freqs-src)]
        #_(println freqs-src)
        (t/is accessed-keys-count
              (min total-entities-in-test crux.query/default-entity-cache-size))
        (println "outside cache frequencies" (vals freqs-src))
        (println "inside cache frequencies" (vals @crux.lru/inside-called-keys))))))

