(ns crux.bootstrap.cluster-node
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.bootstrap :as b]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.index :as idx]
            [crux.kafka :as k]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.tx :as tx])
  (:import java.io.Closeable
           crux.api.ICruxAPI))

(s/def :crux.http-server/server-port :crux.io/port)

(s/def ::options (s/keys :opt-un [:crux.kafka/bootstrap-servers
                                  :crux.kafka/group-id
                                  :crux.kafka/tx-topic
                                  :crux.kafka/doc-topic
                                  :crux.kafka/doc-partitions
                                  :crux.kaka/create-topics
                                  :crux.kafka/replication-factor
                                  :crux.kv/db-dir
                                  :crux.kv/kv-backend
                                  :crux.http-server/server-port
                                  :crux.tx-log/await-tx-timeout
                                  :crux.lru/doc-cache-size
                                  :crux.db/object-store]))

(defn- read-kafka-properties-file [f]
  (when f
    (with-open [in (io/reader (io/file f))]
      (cio/load-properties in))))

(defn start-cluster-node ^ICruxAPI [options]
  (let [options (merge b/default-options options)
        _ (s/assert ::options options)
        {:keys [bootstrap-servers
                group-id
                tx-topic
                doc-topic
                server-port
                kafka-properties-file
                kafka-properties-map
                doc-cache-size]
         :as options
         :or {doc-cache-size (:doc-cache-size b/default-options)}} options
        kafka-config (merge {"bootstrap.servers" bootstrap-servers}
                            (read-kafka-properties-file kafka-properties-file)
                            kafka-properties-map)
        producer-config kafka-config
        consumer-config (merge {"group.id" (:group-id options)}
                               kafka-config)

        _ (log/info "starting system")

        kv-store (b/start-kv-store options)
        producer (k/create-producer producer-config)
        tx-log ^java.io.Closeable (k/->KafkaTxLog producer tx-topic doc-topic kafka-config)
        object-store ^java.io.Closeable (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                                                 (b/start-object-store {:kv kv-store} options))
        indexer ^java.io.Closeable (tx/->KvIndexer kv-store tx-log object-store)
        admin-client (k/create-admin-client kafka-config)
        indexing-consumer (k/start-indexing-consumer admin-client consumer-config indexer options)]
    (log/info "system started")
    (b/map->CruxNode {:kv-store kv-store
                      :producer producer
                      :tx-log tx-log
                      :object-store object-store
                      :indexer indexer
                      :admin-client admin-client
                      :consumer-config consumer-config
                      :indexing-consumer indexing-consumer
                      :options options
                      :close-fn (fn []
                                  (doseq [c [kv-store producer tx-log object-store indexer indexing-consumer]]
                                    (log/info "stopping system")
                                    (cio/try-close c))
                                  (.close admin-client))})))
