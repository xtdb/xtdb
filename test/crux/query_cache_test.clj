(ns crux.query-cache-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.bench :as bench-tools]
            [crux.api :as api]))

(def currencies
  (mapv #(assoc % :crux.db/id (:currency/id %))
        [{:currency/id :currency.id/eur
          :currency/name "Euro"}
         {:currency/id :currency.id/usd
          :currency/name "US Dollar"}
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

(defn gen-stock [i]
  (let [id (keyword "stock.id" (str "company-" i))]
    {:stock/id id
     :crux.db/id id
     :stock/price (rand-int 1000)
     :stock/currency-id (:currency/id (rand-nth currencies))}))

(def stocks-count 100000)

(defn with-stocks-data [f]
  (api/submit-tx f/*api* (f/maps->tx-ops currencies))
  (println "stocks count" stocks-count)
  (let [ctr (atom 0)
        partitions-total (/ stocks-count 1000)]
    (doseq [stocks-batch (partition-all 1000 (map gen-stock (range stocks-count)))]
      (println "partition " @ctr "/" partitions-total)
      (swap! ctr inc)
      (api/submit-tx f/*api* (f/maps->tx-ops stocks-batch))))
  (api/sync f/*api* (java.time.Duration/ofMinutes 2))
  (f))

(t/use-fixtures :once
                f/with-embedded-kafka-cluster
                f/with-kafka-client
                f/with-cluster-node
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
  '[:find stock-id currency-name
    :where
    [stock-id :stock/currency-id currency-id]
    [currency-id :currency/name currency-name]])

(def sample-size 10)

(defn not-really-benchmarking [db n]
  (for [_ (range n)]
    (crux.bench/duration-millis (api/q db query))))

(t/deftest test-stocks-query
  (println "running a stocks query with join to currencies")

  (binding [crux.query/*with-entities-cache?* false]
    (let [db (api/db f/*api*)
          -dry (crux.bench/duration-millis (api/q db query))
          durations (not-really-benchmarking db sample-size)]
      (println "without cache durations in millis are" durations "avg" (/ (apply + durations) sample-size))))

  (binding [crux.query/*with-entities-cache?* true]
    (let [db (api/db f/*api*)
          -dry (crux.bench/duration-millis (api/q db query))
          durations (not-really-benchmarking db sample-size)]
      (println "with cache durations in millis are" durations "avg" (/ (apply + durations) sample-size))))

  (t/is stocks-count (count (api/q (api/db f/*api*) query))))

