(ns crux-microbench.cached-entities-index-in-vivo
  "Microbench for cached entities index in a more realistic usage scenario.
  With some naive file persistence, as test can take a long time and sometimes crash.
  `bench-with-data-plane` should just work"
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux-microbench.ticker-data-gen :as data-gen]
            [crux.api :as api]
            [crux.bench]
            [crux.fixtures :as f]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :as f-api]
            [clojure.java.io :as io]
            [clojure.pprint :as pp])
  (:import (java.sql Date)
           (java.time Duration)))


; ----- Helpers -----
(def ^:private sync-times (atom {}))
(def ^:private query-perf-times (atom {}))

(defn- slurp-edn [filename]
  (if (.exists (io/as-file filename))
    (read-string (slurp filename))))

(defn- slurp-test-times! []
  (reset! query-perf-times (or (slurp-edn "test-times.edn") {}))
  (reset! sync-times (or (slurp-edn "sync-times.edn") {})))

(defn- spit-test-times! []
  (spit "sync-times.edn" (pr-str @sync-times))
  (spit "test-times.edn" (pr-str @query-perf-times)))

(defn avg [nums]
  (/ (reduce + nums) (count nums)))


; ----- Bench settings and vars -----
(def ^:private query-size 100000)
(def ^:dynamic stocks-count 1000)
(def ^:dynamic history-days 1)
(def ^:private sample-size 20)

(def ^:private query-1
  '[:find stock-id currency-id
    :where
    [currency-id :currency/name currency-name]
    [stock-id :stock/currency-id currency-id]])

(def ^:private query-2
  '[:find stock-id currency-name backup-currency-name
    :where
    [currency-id :currency/name currency-name]
    [backup-currency-id :currency/name backup-currency-name]
    [stock-id :stock/currency-id currency-id]
    [stock-id :stock/backup-currency-id backup-currency-id]])


; ----- payload -----
(defn- upload-stocks-with-history [crux-node]
  (let [base-stocks (map data-gen/gen-ticker (range stocks-count))
        base-date #inst "2019"
        partitions-total (/ stocks-count 1000)]
    (doseq [i (range history-days)
            :let [date (Date. ^long (+ (.getTime base-date) (* 1000 60 60 24 i)))
                  ctr (atom 0)
                  stocks-batch (data-gen/alter-tickers base-stocks)
                  t-currencies (map data-gen/alter-currency data-gen/currencies)]]
      (doseq [stocks-batch (partition-all 1000 stocks-batch)]
        (swap! ctr inc)
        (println "day " (inc i) "\t" "partition " @ctr "/" partitions-total)
        (api/submit-tx crux-node (f/maps->tx-ops stocks-batch)))
      (api/submit-tx crux-node (f/maps->tx-ops t-currencies date)))
    (println "Txes submitted, synchronizing...")
    (let [sync-time
          (crux.bench/duration-millis
            (api/sync crux-node (Duration/ofMinutes 20)))]
      (swap! sync-times assoc [stocks-count history-days] sync-time)
      (println "Sync takes: " sync-time))))

(defn- naive-durations-measure [query db n]
  ; naive warmup
  (api/q db query)
  (api/q db query)
  (api/q db query)
  (for [_ (range n)]
    (crux.bench/duration-millis (api/q db query))))

(defn- naively-bench-query [test-id query query-id cache-on?]
  (binding [crux.query/*with-entities-cache?* cache-on?]
    (let [db (api/db *api*)
          durations (naive-durations-measure query db sample-size)
          avg (avg durations)
          res
          {:durations durations
           :avg avg}]
      (println test-id :durations durations)
      (println test-id :avg avg)
      #_(println "res-size" (count res))
      #_(println "rand-sample" (take 10 (random-sample 0.1 res)))
      (swap! query-perf-times assoc [stocks-count history-days query-id cache-on?] res))))

(defn- res->matrix [values]
  (let [hday-set   (->> (map :history-days values) set sort)
        scount-set (->> (map :stocks-count values) set sort)]
    {:x {:title "History size (10x)" ; hd
         :ticks hday-set}
     :y {:title "Stocks count (1000x)"
         :ticks scount-set}
     :data
     (for [sc scount-set
           :let [local-values (filter #(= sc (:stocks-count %)) values)]]
       (for [hd hday-set]
         (:avg (first (filter #(= hd (:history-days %)) local-values)) 0)))}))

; (res->matrix t)

(defn- with-matrix [[k v]]
  [k (res->matrix v)])

(defn- untangle-plot-data [plot-data]
  (let [entry->v
        (fn [[k v]]
          (let [[stocks-count history-days query-id cache-on?] k]
            (-> v
                (dissoc :durations)
                (assoc  :stocks-count stocks-count
                        :query-id query-id
                        :cache-on? cache-on?
                        :history-days history-days))))
        mash (map entry->v plot-data)
        key-fn (fn [{:keys [cache-on? query-id]}]
                 (keyword (str (name query-id) (if cache-on? "-with-cache"))))
        per-query-data (group-by key-fn mash)
        plots-data (into {} (map with-matrix per-query-data))]
    plots-data))

; (untangle-plot-data (read-string (slurp "test-times.edn")))

(defn- with-tickers-and-history [f]
  (upload-stocks-with-history *api*)
  (f))

(defn- with-local-setup [f]
  (let [f'
          (->>
            (partial with-tickers-and-history f)
            (partial f-api/with-node)
            (partial fk/with-cluster-node-opts)
            (partial fk/with-kafka-client)
            (partial fk/with-embedded-kafka-cluster))]
    (f')))

(defn ^:microbench bench-with-data-plane []
  (slurp-test-times!)
  (doseq [tickers-count (range 1000 2000 1000)
          history-entries (cons 1 (range 10 20 10))
          :let [q-key [tickers-count history-entries ::q1 true]]]
    (if-not false ;(contains? @test-times [tickers-count history-entries ::q1 true])
      (binding [stocks-count tickers-count
                history-days history-entries]
        (println "starting a case for " tickers-count history-entries)
        (with-local-setup
          (fn []
            (naively-bench-query q-key query-2 ::q2 false)
            (naively-bench-query q-key query-2 ::q2 true)))
        (spit-test-times!))
      (println :skipping-key q-key)))
  (spit "plot-data-query.edn" (pp/pprint (untangle-plot-data @query-perf-times))))


(comment

  (bench-with-data-plane)

  (pp/pprint @sync-times)

  (reset! query-perf-times (read-string (slurp "test-times.edn")))
  (get @query-perf-times [10000 1 ::q2 false])
  (def s (read-string (slurp "plots-data.edn")))
  (def q2 (::q2 s))
  (def q2-data (:data q2))
  (clojure.pprint/pprint q2-data))

