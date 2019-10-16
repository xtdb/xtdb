(ns crux-microbench.ticker-data-gen)

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

(defn gen-ticker [i]
  (let [id (keyword "stock.id" (str "company-" i))]
    {:stock/id id
     :crux.db/id id
     :stock/price (rand-int 1000)
     :stock/currency-id (:currency/id (rand-nth currencies))
     :stock/backup-currency-id (:currency/id (rand-nth currencies))}))

(defn gen-tickers [n]
  (map gen-ticker (range n)))

(defn alter-ticker [ticker]
  (assoc ticker :stock/price (rand-int 1000)))

(defn alter-currency [currency]
  (update currency :currency/ratio-to-euro + (* 0.1 (rand))))

(defn alter-tickers [pack]
  (map alter-ticker pack))
