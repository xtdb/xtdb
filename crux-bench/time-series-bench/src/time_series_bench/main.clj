(ns time-series-bench.main
  (:require [time-series-bench.ts-weather :as weather]
            [time-series-bench.ts-devices :as devices]
            [clojure.data.json :as json]))

(defn -main []
  (let [start-time (System/currentTimeMillis)]
    (println (json/write-str {:crux.bench/bench-type ::main
                              ::message "Starting devices benchmark"
                              ::sys-time (System/currentTimeMillis)}))
    (devices/bench)
    (println (json/write-str {:crux.bench/bench-type ::main
                              ::message "Starting weather benchmark"
                              ::sys-time (System/currentTimeMillis)}))
    (weather/bench)
    (println (json/write-str (let [end-time (System/currentTimeMillis)]
                               {:crux.bench/bench-type ::main
                                ::message "Fishined benchmark"
                                ::sys-time end-time
                                ::time-taken (- end-time start-time)})))))
