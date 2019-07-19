(ns crux.standalone
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

(s/def ::event-log-sync-interval-ms nat-int?)
(s/def ::event-log-fsync-opts (s/keys :opt-un [:crux.kv/sync?]
                                      :opt [::event-log-sync-interval-ms]))

(defn- start-event-log-fsync ^java.io.Closeable [{:keys [event-log-kv]}
                                                 {:keys [sync? crux.standalone/event-log-sync-interval-ms]}]
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
        (some-> fsync-thread (.join fsync-thread))))))

(defmethod b/define-module ::event-log-sync [_] [start-event-log-fsync [:event-log-kv] ::event-log-fsync-opts])

(s/def ::event-log-dir string?)
(s/def ::event-log-kv-backend :crux.kv/kv-backend)
(s/def ::event-log-kv-opts (s/keys :req-un [:crux.kv/db-dir
                                            ::event-log-dir]
                                   :opt-un [:crux.kv/kv-backend
                                            :crux.kv/sync?]
                                   :opt [::event-log-sync-interval-ms
                                         ::event-log-kv-backend]))

(defn- start-event-log-kv [_ {:keys [crux.standalone/event-log-kv-backend
                                     crux.standalone/event-log-sync-interval-ms
                                     event-log-dir kv-backend sync?]}]
  (let [event-log-sync? (boolean (or sync? (not event-log-sync-interval-ms)))]
    (b/start-kv-store
     {:db-dir event-log-dir
      :kv-backend (or event-log-kv-backend kv-backend)
      :sync? event-log-sync?
      :crux.index/check-and-store-index-version false})))

(defmethod b/define-module ::event-log-kv [_] [start-event-log-kv [] ::event-log-kv-opts])

(defn- start-event-log-consumer [{:keys [event-log-kv indexer]} _]
  (when event-log-kv
    (p/start-event-log-consumer indexer
                                (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                     :batch-size 100}))))

(defmethod b/define-module ::event-log-consumer [_] [start-event-log-consumer [:event-log-kv :indexer]])

(defn- start-moberg-event-log [{:keys [event-log-kv]} _]
  (moberg/->MobergTxLog event-log-kv))

(defmethod b/define-module ::tx-log [_] [start-moberg-event-log [:event-log-kv]])

(def node-config (merge b/base-node-config
                        {:event-log-kv ::event-log-kv
                         :event-log-sync ::event-log-sync
                         :event-log-consumer ::event-log-consumer
                         :tx-log ::tx-log}))

(defn start-standalone-node ^ICruxAPI [options]
  (b/start-node node-config (merge b/default-options options)))
