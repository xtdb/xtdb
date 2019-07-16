(ns crux.bootstrap.standalone
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [crux.backup :as backup]
            [crux.bootstrap :as b]
            [crux.moberg :as moberg]
            [crux.tx.polling :as p]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.tx :as tx])
  (:import java.io.Closeable
           crux.api.ICruxAPI
           crux.bootstrap.CruxNode))

(s/def ::standalone-options (s/keys :req-un [:crux.kv/db-dir :crux.tx/event-log-dir :crux.kv/kv-backend]
                                    :opt-un [:crux.kv/sync? :crux.db/object-store :crux.lru/doc-cache-size]
                                    :opt [:crux.tx/event-log-sync-interval-ms
                                          :crux.tx/event-log-kv-backend]))

(defn- start-event-log-fsync ^java.io.Closeable [event-log-kv event-log-sync-interval-ms]
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
        (.join fsync-thread)))))

(defn start-standalone-node ^ICruxAPI [options]
  (s/assert ::standalone-options options)
  (let [{:keys [db-dir sync? kv-backend event-log-dir doc-cache-size
                crux.tx/event-log-kv-backend crux.tx/event-log-sync-interval-ms] :as options}
        (merge b/default-options options)
        started (atom [])]
    (try
      (let [kv-store (doto (b/start-kv-store
                            (merge (when-not event-log-dir
                                     {:sync? true})
                                   options))
                       (->> (swap! started conj)))
            event-log-sync? (boolean (or sync? (not event-log-sync-interval-ms)))
            event-log-kv-store (doto (b/start-kv-store
                                      {:db-dir event-log-dir
                                       :kv-backend (or event-log-kv-backend kv-backend)
                                       :sync? event-log-sync?
                                       :crux.index/check-and-store-index-version false})
                                 (->> (swap! started conj)))

            object-store (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                                  (b/start-object-store {:kv kv-store} options))

            tx-log (doto (moberg/->MobergTxLog event-log-kv-store)
                     (->> (swap! started conj)))

            indexer (tx/->KvIndexer kv-store tx-log object-store)

            event-log-consumer (when event-log-kv-store
                                 (doto (p/start-event-log-consumer indexer
                                                                   (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv-store
                                                                                                        :batch-size 100}))
                                   (->> (swap! started conj))))

            event-log-fsync (when (and sync? event-log-sync-interval-ms)
                              (doto (start-event-log-fsync event-log-kv-store event-log-sync-interval-ms)
                                (->> (swap! started conj))))]

        (b/map->CruxNode {:kv-store kv-store
                          :tx-log tx-log
                          :object-store object-store
                          :indexer indexer
                          :options options
                          :close-fn (fn []
                                      (doseq [c [event-log-consumer event-log-fsync tx-log kv-store]]
                                        (when c
                                          (cio/try-close c))))}))
      (catch Throwable t
        (doseq [c (reverse @started)]
          (cio/try-close c))
        (throw t)))))
