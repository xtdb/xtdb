(ns crux.bootstrap.jdbc
  (:require [clojure.spec.alpha :as s]
            [crux.bootstrap :as b]
            [crux.lru :as lru]
            [crux.tx :as tx]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [crux.jdbc :as j])
  (:import crux.api.ICruxAPI
           crux.bootstrap.CruxNode))

(s/def ::options (s/keys :req-un [::dbtype
                                  ::dbname]))

(defn- setup-schema! [ds]
  (jdbc/execute! ds ["
  create table if not exists tx_events (
  offset int auto_increment PRIMARY KEY,
  id VARCHAR,
  tx_time datetime default CURRENT_TIMESTAMP,
  topic VARCHAR NOT NULL,
  v BINARY NOT NULL)"]))

(defn ^ICruxAPI start-jdbc-system [{:keys [dbtype
                                           dbname]
                                    :as options}]
  (s/assert ::options options)
  (log/info "starting system")
  (let [{:keys [doc-cache-size] :as options} (merge b/default-options options)
        ds (jdbc/get-datasource options)
        _ (setup-schema! ds)
        kv-store (b/start-kv-store options)
        object-store (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                              (b/start-object-store {:kv kv-store} options))
        tx-log (j/map->JdbcTxLog {:ds ds})
        indexer (tx/->KvIndexer kv-store tx-log object-store)
        event-log-consumer (j/start-event-log-consumer ds indexer)]

    (b/map->CruxNode {:kv-store kv-store
                      :tx-log tx-log
                      :object-store object-store
                      :indexer indexer
                      :event-log-consumer event-log-consumer
                      :options options
                      :close-fn (fn []
                                  (cio/try-close event-log-consumer kv-store))})))
