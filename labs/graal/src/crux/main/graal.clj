(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.api :as xt])
  (:gen-class))

(defn -main [& _args]
  (with-open [node (xt/start-node {})]
    (log/info "Starting Crux native image" (pr-str (.status node)))))
