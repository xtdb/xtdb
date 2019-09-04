(ns crux.bootstrap.jdbc
  (:require [clojure.spec.alpha :as s]
            [crux.bootstrap :as b]
            [crux.lru :as lru]
            [crux.tx :as tx]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [crux.tx.polling :as p]
            [crux.jdbc :as j])
  (:import crux.api.ICruxAPI
           crux.bootstrap.CruxNode
           crux.jdbc.JDBCEventLogConsumer))

(s/def ::options (s/keys :req-un [::dbtype
                                  ::dbname]))

(defn- setup-schema! [ds dbtype]
  (jdbc/execute! ds [(condp contains? dbtype
                       #{"h2"}
                       "create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR,
  tx_time datetime default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v BINARY NOT NULL)"

                       #{"postgresql" "pgsql"}
                       "create table if not exists tx_events (
  event_offset serial PRIMARY KEY, event_key VARCHAR,
  tx_time timestamp default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v bytea NOT NULL)")]))

(defn ^ICruxAPI start-jdbc-node [{:keys [dbtype
                                           dbname]
                                    :as options}]
  (s/assert ::options options)
  (log/info "starting node")
  (let [{:keys [doc-cache-size] :as options} (merge b/default-options options)
        ds (jdbc/get-datasource options)
        _ (setup-schema! ds dbtype)
        kv-store (b/start-kv-store options)
        object-store (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                              (b/start-object-store {:kv kv-store} options))
        tx-log (j/map->JdbcTxLog {:ds ds :dbtype dbtype})
        indexer (tx/->KvIndexer kv-store tx-log object-store)
        event-log-consumer (p/start-event-log-consumer indexer (JDBCEventLogConsumer. ds))]

    (b/map->CruxNode {:kv-store kv-store
                      :tx-log tx-log
                      :object-store object-store
                      :indexer indexer
                      :event-log-consumer event-log-consumer
                      :options options
                      :close-fn (fn []
                                  (doseq [c [event-log-consumer kv-store]]
                                    (cio/try-close c)))})))
