(ns ^:no-doc crux.rocksdb.metrics
  (:require [clojure.string :as str]
            [crux.metrics :as m]
            [crux.metrics.dropwizard :as dw]
            [crux.system :as sys]
            [crux.rocksdb :as rocks]
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
                 ::sys/args {:instance-name {:doc "unique name for this instance of RocksDB, used in metrics domains"
                                             :default "rocksdb"
                                             :spec ::sys/string
                                             :required? true}
                             :sample-window {:doc "Sample window of statistics collector"
                                             :default (Duration/ofSeconds 3)
                                             :required? true
                                             :spec ::sys/duration}}}
  [{:keys [registry instance-name sample-window]}]
  (fn [^RocksDB db, ^Statistics stats]
    (let [meters (->> (seq (TickerType/values))
                      (into {} (map (fn [^TickerType ticker-type]
                                      [ticker-type (dw/meter registry [instance-name (ticker-type-name ticker-type)])]))))
          collector (doto (->collector stats
                                       (fn [{:keys [ticker-type ticker-count]}]
                                         (some-> (get meters ticker-type) (dw/mark! ticker-count)))
                                       sample-window)
                      .start)]
      (dw/gauge registry [instance-name "num-snapshots"]
                #(.getLongProperty db "rocksdb.num-snapshots"))
      (reify Closeable
        (close [_]
          (.shutDown collector 1000))))))
