(ns crux.kv.rocksdb.metrics
  (:require [crux.kv.rocksdb :as rocks]
            [clojure.string :as str])
  (:import (org.rocksdb Statistics StatisticsCollector StatsCollectorInput StatisticsCollectorCallback TickerType)
           (java.io Closeable)))

(defn ticker-type-name [^TickerType ticker-type]
  (-> (.name ticker-type) (str/lower-case) (str/replace #"_" "-")))

(def ticker-types
  (->> (seq (TickerType/values))
       (into {} (map (juxt ticker-type-name identity)))))

(defn- ->collector ^org.rocksdb.StatisticsCollector [^Statistics stats]
  (StatisticsCollector. [(StatsCollectorInput. stats
                                               (reify StatisticsCollectorCallback
                                                 (histogramCallback [this histType histData])

                                                 (tickerCallback [this ticker-type ticker-count]
                                                   (when (and (= ticker-type (get ticker-types "bytes-written"))
                                                              (pos? ticker-count))
                                                     (println "tickerCallback" ticker-type (.getAndResetTickerCount stats ticker-type))))))]
                        3000))

(def metrics-module
  {::metrics {:start-fn (fn [{:crux.node/keys [kv-store]} args]
                          (let [collector (doto (->collector (get-in kv-store [:kv :stats]))
                                            .start)]

                            (reify Closeable
                              (close [_]
                                (.shutDown collector 5000)))))

              :deps #{:crux.node/kv-store}}})
