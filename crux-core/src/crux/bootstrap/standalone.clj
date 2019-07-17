(ns crux.bootstrap.standalone
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.bootstrap :as b]
            [crux.moberg :as moberg]
            [crux.tx.polling :as p]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.tx :as tx])
  (:import java.io.Closeable
           crux.api.ICruxAPI))

(defn- start-event-log-fsync ^java.io.Closeable [{:keys [event-log-kv]} {:keys [sync? event-log-sync-interval-ms]}]
  (log/debug "Using event log fsync interval ms:" event-log-sync-interval-ms)
  (let [running? (atom true)
        fsync-thread (when (and sync? event-log-sync-interval-ms)
                       (doto (Thread. #(while @running?
                                         (try
                                           (Thread/sleep event-log-sync-interval-ms)
                                           (kv/fsync event-log-kv)
                                           (catch Throwable t
                                             (log/error t "Event log fsync threw exception:"))))
                                      "crux.tx.event-log-fsync-thread")
                         (.start)))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join fsync-thread)))))

(defn- start-event-log-kv [_ {:keys [event-log-dir crux.tx/event-log-kv-backend kv-backend
                                     sync? crux.tx/event-log-sync-interval-ms]}]
  (let [event-log-sync? (boolean (or sync? (not event-log-sync-interval-ms)))]
    (b/start-kv-store
     {:db-dir event-log-dir
      :kv-backend (or event-log-kv-backend kv-backend)
      :sync? event-log-sync?
      :crux.index/check-and-store-index-version false})))

(defn- start-event-log-consumer [{:keys [event-log-kv indexer]} _]
  (when event-log-kv
    (p/start-event-log-consumer indexer
                                (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                     :batch-size 100}))))

(defn- start-moberg-event-log [{:keys [event-log-kv]} _]
  (moberg/->MobergTxLog event-log-kv))

(defn- start-object-store [{:keys [kv-store]} {:keys [doc-cache-size] :as options}]
  (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                           (b/start-object-store {:kv kv-store} options)))

(defn- start-kv-store [_ {:keys [event-log-dir crux.tx/event-log-sync-interval-ms sync?] :as options}]
  (b/start-kv-store
   (merge (when-not event-log-dir
            {:sync? true})
          options)))

(defn- start-kv-indexer [{:keys [kv-store tx-log object-store]} _]
  (tx/->KvIndexer kv-store tx-log object-store))

(def standalone-node-config {:event-log-kv start-event-log-kv
                             :event-log-sync [start-event-log-fsync :event-log-kv]
                             :event-log-consumer [start-event-log-consumer :event-log-kv :indexer]
                             :tx-log [start-moberg-event-log :event-log-kv]
                             :kv-store start-kv-store
                             :object-store [start-object-store :kv-store]
                             :indexer [start-kv-indexer :kv-store :tx-log :object-store]})

(s/def ::standalone-options (s/keys :req-un [:crux.kv/db-dir :crux.tx/event-log-dir :crux.kv/kv-backend]
                                    :opt-un [:crux.kv/sync? :crux.db/object-store :crux.lru/doc-cache-size]
                                    :opt [:crux.tx/event-log-sync-interval-ms
                                          :crux.tx/event-log-kv-backend]))

(defn start-standalone-node ^ICruxAPI [options]
  (s/assert ::standalone-options options)
  (b/start-node standalone-node-config (merge b/default-options options)))
