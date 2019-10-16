(ns crux.standalone
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.kv :as kv]
            [crux.moberg :as moberg]
            [crux.node :as n]
            [crux.tx.polling :as p])
  (:import java.io.Closeable))

(defn- start-event-log-fsync ^java.io.Closeable [{::keys [event-log-kv]}
                                                 {::keys [event-log-sync-interval-ms
                                                          event-log-sync?]}]
  (assert (not (and event-log-sync? event-log-sync-interval-ms))
          "Cannot specify both event-log-sync-interval and event-log-sync")
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

(defn- start-event-log-kv [_ {:keys [crux.standalone/event-log-kv-store
                                     crux.standalone/event-log-dir
                                     crux.standalone/event-log-sync?]}]
  (let [options {:crux.kv/db-dir event-log-dir
                 :crux.kv/sync? event-log-sync?
                 :crux.kv/check-and-store-index-version false}]
    (n/start-module event-log-kv-store nil options)))

(defn- start-event-log-consumer [{:keys [crux.standalone/event-log-kv crux.node/indexer]} _]
  (when event-log-kv
    (p/start-event-log-consumer indexer
                                (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                     :batch-size 100}))))

(defn- start-moberg-event-log [{::keys [event-log-kv]} _]
  (moberg/->MobergTxLog event-log-kv))

(def topology (merge n/base-topology
                     {::event-log-kv {:start-fn start-event-log-kv
                                      :args {::event-log-kv-store
                                             {:doc "Key/Value store to use for standalone event-log persistence."
                                              :default 'crux.kv.rocksdb/kv
                                              :crux.config/type :crux.config/module}
                                             ::event-log-dir
                                             {:doc "Directory used to store the event-log and used for backup/restore."
                                              :required? true
                                              :crux.config/type :crux.config/string}
                                             ::event-log-sync?
                                             {:doc "Sync the event-log backed KV store to disk after every write."
                                              :default false
                                              :crux.config/type :crux.config/boolean}}}
                      ::event-log-sync {:start-fn start-event-log-fsync
                                        :deps [::event-log-kv]
                                        :args {::event-log-sync-interval-ms
                                               {:doc "Duration in millis between sync-ing the event-log to the K/V store."
                                                :crux.config/type :crux.config/nat-int}}}
                      ::event-log-consumer {:start-fn start-event-log-consumer
                                            :deps [::event-log-kv :crux.node/indexer]}
                      :crux.node/tx-log {:start-fn start-moberg-event-log
                                         :deps [::event-log-kv]}}))
