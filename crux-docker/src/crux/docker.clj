(ns crux.docker
  (:require [crux.api :as crux]
            [crux.http-server :as srv]))

(defn -main []
  (let [node (crux/start-node {:crux.node/topology :crux.standalone/topology
                               :crux.node/kv-store "crux.kv.memdb/kv"
                               :crux.kv/db-dir "crux-node/db"
                               :crux.standalone/event-log-dir "crux-node/events"
                               :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})]
    (srv/start-http-server node)))
