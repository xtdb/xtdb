(ns crux.query-cache-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
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


(defn with-stocks-data [f]
  (api/submit-tx f/*api* (f/maps->tx-ops currencies))
  (doseq [stocks-batch (partition-all 1000 (map gen-stock (range 1000)))]
    (api/submit-tx f/*api* (f/maps->tx-ops stocks-batch)))
  (api/sync f/*api* nil)
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

(t/deftest test-stocks-query
  (println "running a stocks query with join to currencies")
  (t/is 1000000
    (time
      (count
        (api/q (api/db f/*api*) query)))))

