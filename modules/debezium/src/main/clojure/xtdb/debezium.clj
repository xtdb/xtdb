(ns xtdb.debezium
  (:require [clojure.tools.logging :as log]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (xtdb.debezium DebeziumLog DebeziumProcessor)))

(defn start!
  "Starts a CDC ingestion node. Returns an AutoCloseable that tears down
   the subscription, processor, log and node on close."
  [node-opts {:keys [bootstrap-servers topic db-name]}]
  (let [node (let [config (doto (xtn/->config node-opts)
                            (-> (.getCompactor) (.threads 0))
                            (.setServer nil)
                            (.setFlightSql nil))]
               (.open config))
        log (DebeziumLog. bootstrap-servers topic)
        processor (DebeziumProcessor. node db-name (.allocator node))
        subscription (.tailAll log processor -1)]
    (log/info "Debezium CDC node started"
              {:bootstrap-servers bootstrap-servers
               :topic topic :db-name db-name})
    (reify java.lang.AutoCloseable
      (close [_]
        (util/close subscription)
        (util/close processor)
        (util/close log)
        (util/close node)))))
