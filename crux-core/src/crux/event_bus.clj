(ns crux.event-bus
  (:require [clojure.tools.logging :as log]
            [crux.io :as cio])
  (:import [java.io Closeable]
           [java.util.concurrent ExecutorService Executors TimeUnit]))

(defprotocol EventSource
  (listen [_ f] [_ listen-ops f]))

(defprotocol EventSink
  (send [_ event]))

(defrecord EventBus [!listeners]
  EventSource
  (listen [this listen-ops f]
    (let [{::keys [event-types]} listen-ops]
      (swap! !listeners
             conj {:executor (Executors/newSingleThreadExecutor (cio/thread-factory "event-bus-listener"))
                   :f f
                   ::event-types event-types})
      nil))

  (listen [this f]
    (listen this {} f))

  EventSink
  (send [_ {::keys [event-type] :as event}]
    (doseq [{:keys [^ExecutorService executor f ::event-types]} @!listeners]
      (when (or (nil? event-types) (contains? event-types event-type))
        (.submit executor ^Runnable #(f event)))))

  Closeable
  (close [_]
    (doseq [{:keys [^ExecutorService executor]} @!listeners]
      (try
        (.shutdownNow executor)
        (or (.awaitTermination executor 5 TimeUnit/SECONDS)
            (log/warn "event bus listener not shut down after 5s"))
        (catch Exception e
          (log/error e "error closing listener"))))))

(def event-bus
  {:start-fn (fn [deps args]
               (->EventBus (atom #{})))})
