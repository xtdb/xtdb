(ns crux-microbench.cached-entities-index-in-vivo
  "Microbench for cached entities index in a more realistic usage scenario."
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.bench :as bench-tools]
            [crux.api :as api]
            [crux.index :as idx]
            [crux.lru :as lru]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk])
  (:import (java.sql Date)))

(defn avg [nums]
  (/ (reduce + nums) (count nums)))


(def query-size 100000)
(def ^:dynamic stocks-count 1000)
(def ^:dynamic history-days 1)
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

(def sync-times (atom {}))
(def test-times (atom {}))

(defn with-stocks-data [f]
  (api/submit-tx *api* (f/maps->tx-ops currencies))
  (println "stocks count" stocks-count)
  (let [ctr (atom 0)
        stocks (map gen-ticker (range stocks-count))
        partitions-total (/ stocks-count 1000)]
    (reset! -stocks stocks)
    (doseq [stocks-batch (partition-all 1000 stocks)]
      (swap! ctr inc)
      (println "partition " @ctr "/" partitions-total)
      (api/submit-tx *api* (f/maps->tx-ops stocks-batch))))
  (api/sync *api* (java.time.Duration/ofMinutes 20))
  (f))

(defn upload-stocks-with-history [crux-node]
  (let [base-stocks (map gen-ticker (range stocks-count))
        base-date #inst "2019"
        partitions-total (/ stocks-count 1000)]
    (reset! -stocks base-stocks)
    (doseq [i (range history-days)
            :let [date (Date. ^long (+ (.getTime base-date) (* 1000 60 60 24 i)))
                  ctr (atom 0)
                  stocks-batch (alter-tickers base-stocks)
                  t-currencies (map alter-currency currencies)]]
      (doseq [stocks-batch (partition-all 1000 stocks-batch)]
        (swap! ctr inc)
        (println "day " (inc i) "\t" "partition " @ctr "/" partitions-total)
        (api/submit-tx crux-node (f/maps->tx-ops stocks-batch)))
      (api/submit-tx crux-node (f/maps->tx-ops t-currencies date)))
    (println "Txes submitted, synchronizing...")
    (let [sync-time
          (crux.bench/duration-millis
            (api/sync crux-node (java.time.Duration/ofMinutes 20)))]
      (swap! sync-times assoc [stocks-count history-days] sync-time)
      (println "Sync takes: " sync-time))))

(defn with-stocks-history-data [f]
  (upload-stocks-with-history *api*)
  (f))

(t/use-fixtures :once
                fk/with-embedded-kafka-cluster
                fk/with-kafka-client
                fk/with-cluster-node
                with-stocks-history-data)

(defn not-really-benchmarking [query db n]
  (for [_ (range n)]
    (crux.bench/duration-millis (api/q db query))))



(defn- test-query [test-id query query-id cache-on?]
  (binding [crux.query/*with-entities-cache?* cache-on?]
     (let [db (api/db *api*)
           -dry (crux.bench/duration-millis (api/q db query))
           durations (not-really-benchmarking query db sample-size)
           res (api/q db query)
           avg (avg durations)
           res
           {:durations durations
            :avg avg}]
       (println test-id :durations durations)
       (println test-id :avg avg)
       #_(println "res-size" (count res))
       #_(println "rand-sample" (take 10 (random-sample 0.1 res)))
       (swap! test-times assoc [stocks-count history-days query-id cache-on?] res))))


(defn test-cache-frequencies-2 []
  (println "\n")
  (println :days history-days :stocks stocks-count)
  (println)
  (test-query :q1-entity-cache-off query-1 :q1 false)
  (test-query :q1-entity-cache-on  query-1 :q1 true)
  (println "\n")
  (test-query :q3-entity-cache-off query-3 :q3 false)
  (test-query :q3-entity-cache-on  query-3 :q3 true)
  (println "\n")
  (t/is true))


(defn do-plot-data []
  (reset! test-times (read-string (slurp "test-times.edn")))
  (reset! sync-times (read-string (slurp "sync-times.edn")))
  (doseq [sc (range 1000 11000 1000)
          hd (cons 1 (range 10 110 10))]
    (if-not false ;(contains? @test-times [sc hd :q1 true])
      (binding [stocks-count sc
                history-days hd]
        (println "starting a case for " sc hd)
        (t/run-tests 'crux.entity-cache-test)
        (spit "sync-times.edn" (pr-str @sync-times))
        (spit "test-times.edn" (pr-str @test-times)))
      (println :skipping-key [sc hd :q1 true]))))

; (do-plot-data)

; (reset! test-times (read-string (slurp "test-times.edn")))
; (get @test-times [10000 1 :q3 false])

(defn res->matrix [values]
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

(defn with-matrix [[k v]]
  [k (res->matrix v)])

(defn untangle-plot-data [plot-data]
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
    (spit "plots-data.edn" (pr-str plots-data))))

; (untangle-plot-data (read-string (slurp "test-times.edn")))

(comment

  (def s (read-string (slurp "plots-data.edn")))

  (def q3 (:q3 s))

  (def q3-data (:data q3))

  (clojure.pprint/pprint q3-data)

  (require '[crux.api :as api])

  (def node
    (api/start-node
     {:crux.node/topology :crux.standalone/topology
      :crux.kv/db-dir "console-data"
      :crux.standalone/event-log-dir "console-data-log"
      :crux.node/kv-store "crux.kv.rocksdb.RocksKv"}))

  (api/q (api/db node) '{:find [e] :where [[e :crux.db/id]]})

  (defn upload-fiddle-data []
    (binding [history-days 10
              stocks-count 10]
      (upload-stocks-with-history node)))



  (api/history node :stock.id/company-8)

  (api/history-range node :stock.id/company-8)

  (def e-hist (api/history-range node :stock.id/company-8 nil nil nil nil))

  (def e1 (first e-hist))

  (api/document node (:crux.db/content-hash e1))

  (api/entity (api/db node) (:crux.db/id e1))

  (api/entity (api/db node) (:crux.db/id e1))

  (mapv (comp (partial api/document node) :crux.db/content-hash) e-hist)

  (upload-fiddle-data))
