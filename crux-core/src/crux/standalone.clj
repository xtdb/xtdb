(ns crux.standalone
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.bootstrap :as b]
            [crux.kv :as kv]
            [crux.moberg :as moberg]
            [crux.tx.polling :as p])
  (:import java.io.Closeable))

(s/def ::event-log-sync-interval-ms nat-int?)
(s/def ::event-log-fsync-opts (s/keys :opt-un [:crux.kv/sync?]
                                      :opt [::event-log-sync-interval-ms]))

(defn- start-event-log-fsync ^java.io.Closeable [{:keys [event-log-kv]}
                                                 {:keys [sync? crux.standalone/event-log-sync-interval-ms]}]
  (log/debug "Using event log fsync interval ms:" event-log-sync-interval-ms)
  (let [running? (atom true)
        fsync-thread (when event-log-sync-interval-ms
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
        (some-> fsync-thread (.join))))))

(s/def ::event-log-dir string?)
(s/def ::event-log-kv-backend :crux.kv/kv-backend)
(s/def ::event-log-kv-opts (s/keys :req [::event-log-dir ::event-log-kv-backend]
                                   :opt-un [:crux.kv/sync?]
                                   :opt [::event-log-sync-interval-ms]))

(defn- start-event-log-kv [_ {:keys [crux.standalone/event-log-kv-backend
                                     crux.standalone/event-log-sync-interval-ms
                                     crux.standalone/event-log-dir
                                     sync?]}]
  (let [event-log-sync? (boolean (or sync? (not event-log-sync-interval-ms)))]
    (b/start-kv-store
     {:crux.kv/db-dir event-log-dir
      :crux.kv/kv-backend event-log-kv-backend
      :sync? event-log-sync?
      :crux.index/check-and-store-index-version false})))

(defn- start-event-log-consumer [{:keys [event-log-kv indexer]} _]
  (when event-log-kv
    (p/start-event-log-consumer indexer
                                (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                     :batch-size 100}))))

(defn- start-moberg-event-log [{:keys [event-log-kv]} _]
  (moberg/->MobergTxLog event-log-kv))

(def event-log-kv [start-event-log-kv
                   []
                   ::event-log-kv-opts
                   {::event-log-kv-backend
                    {:doc "Key/Value store to use for standalone event-log persistence. If not present, will use `crux.kv/kv-backend."
                     :default "crux.kv.rocksdb.RocksKv"}
                    ::event-log-dir
                    {:doc "Directory used to store the event-log and used for backup/restore."}}])

(def event-log-sync [start-event-log-fsync [:event-log-kv] ::event-log-fsync-opts])
(def event-log-consumer [start-event-log-consumer [:event-log-kv :indexer]])
(def tx-log [start-moberg-event-log [:event-log-kv]])

(def node-config {:event-log-kv event-log-kv
                  :event-log-sync event-log-sync
                  :event-log-consumer event-log-consumer
                  :tx-log tx-log})

(comment
  ;; Start a Standalone node:
  (b/start-node node-config some-options))
