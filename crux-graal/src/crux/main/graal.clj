(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.api :as crux])
  (:import crux.api.ICruxAPI)
  (:gen-class))

(defn -main [& args]
  (with-open [node (crux/start-node {})]
    (log/info "Starting Crux native image" (pr-str (.status node)))))
