(ns crux.kv.rocksdb.metrics
  (:require [clojure.string :as str]
            [crux.metrics.dropwizard :as dw])
  (:import (org.rocksdb Statistics StatisticsCollector StatsCollectorInput StatisticsCollectorCallback TickerType)
           (java.io Closeable)))

(defn ticker-type-name [^TickerType ticker-type]
  (-> (.name ticker-type) (str/lower-case) (str/replace #"_" "-")))

(defn ->collector ^org.rocksdb.StatisticsCollector [^Statistics stats, f, bin-size]
  (StatisticsCollector. [(StatsCollectorInput. stats
                                               (reify StatisticsCollectorCallback
                                                 (histogramCallback [this histType histData])

                                                 (tickerCallback [this ticker-type ticker-count]
                                                   (f {:ticker-type ticker-type
                                                       :ticker-count (.getAndResetTickerCount stats ticker-type)}))))]
                        bin-size))

(def metrics-module
  {::metrics {:start-fn (fn [{:keys [crux.node/kv-store crux.metrics/registry]} {::keys [bin-size shutdown-timeout]}]
                          (let [meters (->> (seq (TickerType/values))
                                            (into {} (map (fn [^TickerType ticker-type]
                                                            [ticker-type (dw/meter registry ["rocksdb" (ticker-type-name ticker-type)])]))))

                                stats (get-in kv-store [:kv :stats])
                                collector (doto (->collector stats
                                                             (fn [{:keys [ticker-type ticker-count]}]
                                                               (some-> (get meters ticker-type) (dw/mark! ticker-count)))
                                                             bin-size)
                                            .start)]

                            (reify Closeable
                              (close [_]
                                (.shutDown collector shutdown-timeout)))))

              :deps #{:crux.node/kv-store :crux.metrics/registry}
              :before #{:crux.metrics/all-metrics-loaded}
              :args {::bin-size {:doc "Sample size of statistics collector in milliseconds"
                                 :default 3000
                                 :crux.config/type :crux.config/int}
                     ::shutdown-timeout {:doc "Time for statistics collector object to finish collecting in milliseconds"
                                         :default 5000
                                         :crux.config/type :crux.config/int}}}})
