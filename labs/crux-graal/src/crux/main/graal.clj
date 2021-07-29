(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.api :as crux])
  (:gen-class))

(defn -main [& _args]
  (with-open [node (crux/start-node {})]
    (log/info "Starting Crux native image" (pr-str (.status node)))))
