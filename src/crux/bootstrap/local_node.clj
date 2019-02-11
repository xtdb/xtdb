(ns crux.bootstrap.local-node
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
           crux.api.ICruxSystem))

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

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(defn run-system [{:keys [bootstrap-servers
                          group-id
                          tx-topic
                          doc-topic
                          server-port
                          kafka-properties-file
                          doc-cache-size]
                   :as options
                   :or {doc-cache-size (:doc-cache-size b/default-options)}}
                  with-system-fn]
  (s/assert ::options options)
  (log/info "starting system")
  (let [kafka-config (merge {"bootstrap.servers" bootstrap-servers}
                            (read-kafka-properties-file kafka-properties-file))
        producer-config kafka-config
        consumer-config (merge {"group.id" (:group-id options)}
                               kafka-config)]
    (with-open [kv-store (b/start-kv-store options)
                producer (k/create-producer producer-config)
                tx-log ^java.io.Closeable (k/->KafkaTxLog producer tx-topic doc-topic kafka-config)
                object-store ^java.io.Closeable (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                                                         (b/start-object-store {:kv kv-store} options))
                indexer ^java.io.Closeable (tx/->KvIndexer kv-store tx-log object-store)
                admin-client (k/create-admin-client kafka-config)
                indexing-consumer (k/start-indexing-consumer admin-client consumer-config indexer options)]
      (log/info "system started")
      (with-system-fn
        {:kv-store kv-store
         :tx-log tx-log
         :producer producer
         :consumer-config consumer-config
         :object-store object-store
         :indexer indexer
         :admin-client admin-client
         :indexing-consumer indexing-consumer})
      (log/info "stopping system")))
  (log/info "system stopped"))

(defn start-local-node ^ICruxSystem [options]
  (let [system-promise (promise)
        close-promise (promise)
        error-promise (promise)
        options (merge b/default-options options)
        node-thread (doto (Thread. (fn []
                                     (try
                                       (run-system
                                        options
                                        (fn with-system-callback [system]
                                          (deliver system-promise system)
                                          @close-promise))
                                       (catch Throwable t
                                         (if (realized? system-promise)
                                           (throw t)
                                           (deliver error-promise t)))))
                                   "crux.bootstrap.local-node.main-thread")
                      (.start))]
    (while (and (nil? (deref system-promise 100 nil))
                (.isAlive node-thread)))
    (when (realized? error-promise)
      (throw @error-promise))
    (b/map->CruxNode (merge {:close-promise close-promise
                             :options options
                             :node-thread node-thread}
                            @system-promise))))
