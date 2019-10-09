(ns crux-ui.logic.example-queries
  (:require [medley.core :as m]
            [clojure.string :as str]
            [crux-ui.functions :as f]
            [crux-ui.logic.example-txes-amzn :as amzn-data]))


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

(defn- gen-vt-str
  ([] (gen-vt-str 0))
  ([offset-in-days]
   (pr-str (gen-vt offset-in-days))))

(def ^:private stock-exchanges
  [{:crux.db/id :se.id/NYSE
    :se/title "New York Stock Exchange"
    :se/currency :currency/usd
    :se/country :se.country/USA}


   ; NASDAQ subsidiaries
   {:crux.db/id :se.id/CSE
    :se/title "Copenhagen Stock Exchange"
    :se/currency :currency/dkk
    :se/country :se.country/Denmark}
   {:crux.db/id  :se.id/STSE
    :se/title    "Stockholm Stock Exchange"
    :se/currency :currency/skk
    :se/country  :se.country/Sweden}
   {:crux.db/id :se.id/HSE
    :se/title "Helsinki Stock Exchange"
    :se/currency :currency/euro
    :se/country :se.country/Finland}
   {:crux.db/id :se.id/ISE
    :se/title "Iceland Stock Exchange"
    :se/currency :currency/skk
    :se/country :se.country/Iceland}
   {:crux.db/id :se.id/TSE
    :se/title "Tallinn Stock Exchange"
    :se/currency :currency/euro
    :se/country :se.country/Estonia}
   {:crux.db/id :se.id/RSE
    :se/title "Riga Stock Exchange"
    :se/currency :currency/euro
    :se/country :se.country/Latvia}
   {:crux.db/id :se.id/VSE
    :se/title "Vilnius Stock Exchange"
    :se/currency :currency/euro
    :se/country :se.country/Lithuania}
   {:crux.db/id :se.id/ASE
    :se/title "Armenian Stock Exchange"
    :se/currency :currency/usd
    :se/country :se.country/Armenia}

   {:crux.db/id :se.id/NASDAQ
    :se/title "NASDAQ"
    :se/country :se.country/USA
    :se/subsidiaries
    #{:se.id/CSE :se.id/STSE :se.id/HSE
      :se.id/ISE :se.id/TSE :se.id/RSE
      :se.id/VSE :se.id/ASE}}

   {:crux.db/id :se.id/JEG
    :se/title "Japan Exchange Group"
    :se/country :se.country/Japan}

   {:crux.db/id :se.id/SSE
    :se/title "Shanghai Stock Exchange"
    :se/country :se.country/China}

   {:crux.db/id :se.id/HKSE
    :se/title "Hong Kong Stock Exchange"
    :se/country :se.country/HongKong}

   {:crux.db/id :se.id/Euronext
    :se/title "Euronext"
    :se/country :se.country/EU}

   {:crux.db/id :se.id/LSE
    :se/title "London Stock Exchange"
    :se/country :se.country/UK}])


(defn- gen-ticker []
  {:crux.db/id     (gen-id)
   :ticker/price   (inc (rand-int 100))
   :ticker/market  (:crux.db/id (rand-nth stock-exchanges))})

(defn- gen-put-with-offset-days [ticker offset-in-days]
  [:crux.tx/put ticker (gen-vt offset-in-days)])

(defn- int-rand-inc [x]
  (+ x (rand-int 7)))

(defn- alter-ticker [ticker amzn-close-price]
  (assoc ticker :ticker/price (- (/ amzn-close-price 15) (* 10 (rand)))))

(def generators
  {:examples/put
   (fn []
     (into (mapv (fn [x] [:crux.tx/put x]) stock-exchanges)
           (mapv (fn [_] [:crux.tx/put (gen-ticker)]) (range 50))))

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
     (str/join "\n"
               ["{:find [e p]"
                " :where"
                " [[e :crux.db/id _]"
                "  [e :ticker/price p]]}"]))

   :examples/query-w-join
   (fn []
     (str/join "\n"
               ["{:find [market-title ticker-id price]"
                " :where"
                " [[ticker-id :ticker/price price]"
                "  [(> price 60)]"
                "  [ticker-id :ticker/market m]"
                "  [m :se/title market-title]]"
                " :order-by [[market-title :asc]]"
                " :limit 10"
                " :offset 1}"]))

   :examples/rules-and-args
   (fn []
     (str/join "\n"
               ["{:find [e p]"
                " :where"
                " [[e :crux.db/id _]"
                "  [e :ticker/price p]"
                "  [(> p p-min)]"
                "  [(<= p p-max)]"
                "  (is-traded-in-currency e :currency/usd)]"
                " :args [{p-min 10  p-max 80}]"
                " :rules"
                " [[(is-traded-in-currency ticker-id curr-id)"
                "   [ticker-id :ticker/market m]"
                "   [m :se/currency curr-id]]]}"]))

   :examples/query-w-full-res
   (fn []
     (str/join "\n"
       ["{:find [e]"
        " :where [[e :crux.db/id _]]"
        " :full-results? true}"]))

   :examples/query-w-full-res-refresh
   (fn []
     (str/join "\n"
               ["{:find  [e]"
                " :where [[e :crux.db/id _]]"
                " :ui/poll-interval-seconds? 30"
                " ;; Refreshes the UI every 30 seconds"
                " ;; to display the most recent data"
                " :full-results? true}"]))

   :examples/vector-style
   (fn []
     (f/lines "[:find e" " :where" " [e :crux.db/id _]" " :full-results? true]"))

   :examples/delete (fn [] [[:crux.tx/delete (get-id)]])
   :examples/evict
   (fn []
     (str "[; evict all entries of the entity \n"
          " [:crux.tx/evict " (get-id) "]\n"
          "\n"
          " ; evict entries from the specified valid time\n"
          " [:crux.tx/evict " (get-id) "\n"
          "  " (gen-vt-str) "]\n"
          "\n"
          " ; evict entries in the specified valid time range\n"
          " [:crux.tx/evict " (get-id) "\n"
          "  " (gen-vt-str -5) "\n"
          "  " (gen-vt-str)"]\n"
          "\n"
          " ; evict entries in the specified vt range\n"
          " ; but keep latest and earliest entries\n"
          " [:crux.tx/evict " (get-id) "\n"
          "  " (gen-vt-str -5) "\n"
          "  " (gen-vt-str) "\n"
          "  true true]]"))})

'{:find [e p],
  :where
  [(or [e :crux.db/id :ids/agriculture-ticker-6]
       [e :crux.db/id :ids/fashion-ticker-2]
       [e :crux.db/id :ids/oil-ticker-2])
   [e :ticker/price p]]}

(def examples
  [{:title ":crux.tx/put"
    :generator (:examples/put generators)}
   {:title "put with valid time"
    :generator (:examples/put-w-valid generators)}
   {:title "simple query"
    :generator (:examples/query generators)}
   {:title "join"
    :generator (:examples/query-w-join generators)}
   {:title "full-results"
    :generator (:examples/query-w-full-res generators)}
   {:title "full-results with refresh"
    :generator (:examples/query-w-full-res-refresh generators)}
   {:title "rules and args"
    :generator (:examples/rules-and-args generators)}
   {:title "delete"
    :generator (:examples/delete generators)}
   {:title "evict"
    :generator (:examples/evict generators)}
   {:title "vector style"
    :generator (:examples/vector-style generators)}])

(defn generate [ex-id]
  (if-let [gen-fn (:generator (m/find-first #(= ex-id (:title %)) examples))]
    (gen-fn)))

