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

(defn start-rocksdb-metrics
  [{:keys [crux.node/kv-store crux.metrics/registry crux.kv.rocksdb/metrics?]} {::keys [bin-size shutdown-timeout]}]

  (when metrics?
    (let [meters (->> (seq (TickerType/values))
                      (into {} (map (fn [^TickerType ticker-type]
                                      [ticker-type (dw/meter registry ["rocksdb" (ticker-type-name ticker-type)])]))))
          stats (get-in kv-store [:kv :stats])
          collector (doto (->collector stats
                                       (fn [{:keys [ticker-type ticker-count]}]
                                         (some-> (get meters ticker-type) (dw/mark! ticker-count)))
                                       bin-size)
                      .start)
          num-snapshots (dw/gauge registry ["rocksdb" "num-snapshots"]
                                  #(.getLongProperty (get-in kv-store [:kv :db]) "rocksdb.num-snapshots"))]
      {:rocks-meters meters
       :num-snapshots num-snapshots}

      (reify Closeable
        (close [_]
          (.shutDown collector shutdown-timeout))))))
