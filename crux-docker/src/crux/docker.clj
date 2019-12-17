(ns crux.docker
  (:require [crux.api :as crux]
            [crux.http-server :as srv]
            [clojure.java.io :as io]))

(defn -main []
  (let [{:keys [crux/node-opts crux/server-opts]} (read-string (slurp (io/file "/etc/crux.edn")))
        node (crux/start-node node-opts)
        cors-opts (:cors-access-control server-opts)
        srv-opts (cond-> server-opts
                   (map? cors-opts) (assoc :cors-access-control (reduce into [] cors-opts)))]
    (srv/start-http-server node srv-opts)))
