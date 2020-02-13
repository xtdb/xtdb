(ns crux.kv.rocksdb.metrics
  (:require [crux.kv.rocksdb :as rocks]
            [clojure.string :as str]
            [crux.metrics.dropwizard :as dw])
  (:import (org.rocksdb Statistics StatisticsCollector StatsCollectorInput StatisticsCollectorCallback TickerType)
           (java.io Closeable)))

(defn ticker-type-name [^TickerType ticker-type]
  (-> (.name ticker-type) (str/lower-case) (str/replace #"_" "-")))

(defn ->collector ^org.rocksdb.StatisticsCollector [^Statistics stats, f]
  (StatisticsCollector. [(StatsCollectorInput. stats
                                               (reify StatisticsCollectorCallback
                                                 (histogramCallback [this histType histData])

                                                 (tickerCallback [this ticker-type ticker-count]
                                                   (f {:ticker-type ticker-type
                                                       :ticker-count (.getAndResetTickerCount stats ticker-type)}))))]
                        ;; TODO configure this rate with a duration arg
                        3000))

(def metrics-module
  {::metrics {:start-fn (fn [{:keys [crux.node/kv-store crux.metrics/registry]} args]
                          (let [meters (->> (seq (TickerType/values))
                                            (into {} (map (fn [^TickerType ticker-type]
                                                            [ticker-type (dw/meter registry ["rocksdb" (ticker-type-name ticker-type)])]))))

                                stats (get-in kv-store [:kv :stats])
                                collector (doto (->collector stats (fn [{:keys [ticker-type ticker-count]}]
                                                                     (some-> (get meters ticker-type) (dw/mark! ticker-count))))
                                            .start)]

                            (reify Closeable
                              (close [_]
                                (.shutDown collector 5000)))))

              :deps #{:crux.node/kv-store :crux.metrics/registry}
              :before #{:crux.metrics/all-metrics-loaded}}})
