(ns juxt.crux-ui.frontend.logic.example-queries
  (:require [medley.core :as m]
            [juxt.crux-ui.frontend.logic.example-txes-amzn :as amzn-data]))


(def currencies
  [:currency/usd
   :currency/eur
   :currency/gbp
   :currency/chf
   :currency/rub
   :currency/yen
   :currency/cny])

(def industries
  [:pharma
   :tech
   :oil
   :agriculture
   :chem
   :industry
   :fashion])


(def ctr (atom 1))

(defn get-ctr []
  (swap! ctr inc)
  @ctr)

(def used-ids (atom []))

(defn- -gen-id []
  (keyword 'ids (str (name (rand-nth industries)) "-ticker-" (get-ctr))))

(defn- gen-id []
  (let [id (-gen-id)]
    (swap! used-ids conj id)
    id))

(defn- get-id []
  (if (empty? @used-ids)
    (-gen-id)
    (rand-nth @used-ids)))

(def ^{:private true :const true} day-millis (* 24 60 60 1000))

(defn- gen-vt
  ([] (gen-vt 0))
  ([offset-in-days]
   (js/Date. (+ (js/Date.now) (* offset-in-days day-millis)))))

(defn- gen-ticker []
  {:crux.db/id        (gen-id)
   :ticker/price      (inc (rand-int 100))
   :ticker/currency   (rand-nth currencies)})

(defn- gen-put-with-offset-days [ticker offset-in-days]
  [:crux.tx/put ticker (gen-vt offset-in-days)])

(defn- int-rand-inc [x]
  (+ x (rand-int 7)))

(defn- alter-ticker [ticker amzn-close-price]
  (assoc ticker :ticker/price amzn-close-price))

(def generators
  {:examples/put (fn [] [[:crux.tx/put (gen-ticker)]])
   :examples/put-10 (fn [] (mapv (fn [_] [:crux.tx/put (gen-ticker)]) (range 10)))
   :examples/put-w-valid
   (fn []
     (let [ticker (gen-ticker)
           mfn (fn [offset-days amzn-close-price]
                 (gen-put-with-offset-days
                   (alter-ticker ticker amzn-close-price)
                   offset-days))]
       (mapv mfn (range -500 1 1) (reverse amzn-data/amzn-close-prices))))

   :examples/query
   (fn []
     '{:find [e p]
       :where [[e :crux.db/id _]
               [e :ticker/price p]]})

   :examples/query-w-full-res
   (fn []
     '{:find [e]
       :where [[e :crux.db/id _]]
       :full-results? true})

   :examples/delete (fn [] [[:crux.tx/delete (get-id)]])
   :examples/evict (fn [] [[:crux.tx/evict (get-id)]])
   :examples/evict-w-valid (fn [] [[:crux.tx/evict (get-id) (gen-vt)]])})

'{:find [e p],
  :where
  [(or [e :crux.db/id :ids/agriculture-ticker-6]
       [e :crux.db/id :ids/fashion-ticker-2]
       [e :crux.db/id :ids/oil-ticker-2])
   [e :ticker/price p]]}

(def examples
  [{:title "[crux.tx/put :some-data]"
    :generator (:examples/put generators)}
   {:title "put 10"
    :generator (:examples/put-10 generators)}
   {:title "put with valid time"
    :generator (:examples/put-w-valid generators)}
   {:title "simple query"
    :generator (:examples/query generators)}
   {:title "query with full-results"
    :generator (:examples/query-w-full-res generators)}
   {:title "delete"
    :generator (:examples/delete generators)}
   {:title "evict"
    :generator (:examples/evict generators)}
   {:title "evict with vt"
    :generator (:examples/evict-w-valid generators)}])

(defn generate [ex-id]
  (if-let [gen-fn (:generator (m/find-first #(= ex-id (:title %)) examples))]
    (gen-fn)))

