(ns crux.bench.main
  (:require [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [crux.bench.utils :as utils]))

(defn -main []
  (let [start-time (System/currentTimeMillis)]
    (utils/output {:crux.bench/bench-type ::main
                   ::message "Starting devices benchmark"
                   ::sys-time (System/currentTimeMillis)})
    (devices/bench)
    (utils/output {:crux.bench/bench-type ::main
                   ::message "Starting weather benchmark"
                   ::sys-time (System/currentTimeMillis)})
    (weather/bench)
    (utils/output (let [end-time (System/currentTimeMillis)]
                    {:crux.bench/bench-type ::main
                     ::message "Fishined benchmark"
                     ::sys-time end-time
                     ::time-taken (- end-time start-time)}))))
