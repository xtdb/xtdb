(ns crux.docker
  (:require [crux.api :as crux]
            [crux.http-server :as srv]
            [clojure.java.io :as io]))

(defn -main []
  (let [{:keys [crux/node-opts crux/server-opts]} (read-string (slurp (io/file "/etc/crux.edn")))
        node (crux/start-node node-opts)
        cors-opts (:cors-access-control server-opts)
        srv-opts (if (map? cors-opts)
                   (assoc server-opts :cors-access-control (reduce into [] cors-opts))
                   server-opts)]
    (srv/start-http-server node srv-opts)))
