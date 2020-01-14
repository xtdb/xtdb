(ns crux.bench.main
  (:require [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [crux.bench.utils :as utils]
            [clojure.tools.logging :as log]))

(defn -main []
  (log/info "Staring ts-devices bench")
  (utils/bench devices/submit-ts-devices-data devices/run-queries :ts-devices)
  (log/info "Starting ts-weather bench")
  (utils/bench weather/submit-ts-weather-data weather/run-queries :ts-weather)
  (log/info "Finished bench"))
