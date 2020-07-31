(ns ^:no-doc crux.kv.rocksdb.metrics
  (:require [clojure.string :as str]
            [crux.metrics :as m]
            [crux.metrics.dropwizard :as dw]
            [crux.system :as sys]
            [crux.kv.rocksdb :as rocks]
            [clojure.set :as set])
  (:import (org.rocksdb RocksDB Statistics StatisticsCollector StatsCollectorInput StatisticsCollectorCallback TickerType)
           (java.io Closeable)
           java.time.Duration))

(defn ticker-type-name [^TickerType ticker-type]
  (-> (.name ticker-type) (str/lower-case) (str/replace #"_" "-")))

(defn ->collector ^org.rocksdb.StatisticsCollector [^Statistics stats, f, ^Duration sample-window]
  (StatisticsCollector. [(StatsCollectorInput. stats
                                               (reify StatisticsCollectorCallback
                                                 (histogramCallback [this histType histData])

                                                 (tickerCallback [this ticker-type ticker-count]
                                                   (f {:ticker-type ticker-type
                                                       :ticker-count (.getAndResetTickerCount stats ticker-type)}))))]
                        (.toMillis sample-window)))

(defn ->metrics {::sys/deps {:registry ::m/registry}
                 ::sys/args {:sample-window {:doc "Sample window of statistics collector in milliseconds"
                                             :default (Duration/ofSeconds 3)
                                             :spec ::sys/duration}}}
  [{:keys [registry sample-window]}]
  (fn [^RocksDB db, ^Statistics stats]
    (let [meters (->> (seq (TickerType/values))
                      (into {} (map (fn [^TickerType ticker-type]
                                      [ticker-type (dw/meter registry ["rocksdb" (ticker-type-name ticker-type)])]))))
          collector (doto (->collector stats
                                       (fn [{:keys [ticker-type ticker-count]}]
                                         (some-> (get meters ticker-type) (dw/mark! ticker-count)))
                                       sample-window)
                      .start)]
      (dw/gauge registry ["rocksdb" "num-snapshots"]
                #(.getLongProperty db "rocksdb.num-snapshots"))
      (reify Closeable
        (close [_]
          (.shutDown collector 1000))))))
