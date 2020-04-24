(ns ^:no-doc crux.bus
  (:refer-clojure :exclude [send])
  (:require [crux.io :as cio]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [java.io Closeable]
           [java.util.concurrent ExecutorService Executors TimeUnit]))

(defprotocol EventSource
  (listen ^java.io.Closeable [_ listen-ops f]))

(defprotocol EventSink
  (send [_ event]))

(s/def :crux/event-type keyword?)

(defmulti event-spec :crux/event-type, :default ::default)
(defmethod event-spec ::default [_] any?)

(s/def ::event (s/and (s/keys :req [:crux/event-type])
                      (s/multi-spec event-spec :crux/event-type)))

(defrecord EventBus [!listeners]
  EventSource
  (listen [this listen-ops f]
    (let [{:crux/keys [event-type]} listen-ops
          listener {:executor (Executors/newSingleThreadExecutor (cio/thread-factory "bus-listener"))
                    :f f
                    :crux/event-type event-type}]
      (swap! !listeners update event-type (fnil conj #{}) listener)
      (reify Closeable
        (close [_]
          (swap! !listeners update event-type disj listener)))))

  EventSink
  (send [_ {:crux/keys [event-type] :as event}]
    (s/assert ::event event)

    (doseq [{:keys [^ExecutorService executor f] :as listener} (get @!listeners event-type)]
      (.submit executor ^Runnable #(f event))))

  Closeable
  (close [_]
    (doseq [{:keys [^ExecutorService executor]} (->> @!listeners (mapcat val))]
      (try
        (.shutdown executor)
        (or (.awaitTermination executor 5 TimeUnit/SECONDS)
            (.shutdownNow executor)
            (log/warn "event bus listener not shut down after 5s"))
        (catch Exception e
          (log/error e "error closing listener"))))))

(def bus
  {:start-fn (fn [deps args]
               (->EventBus (atom {})))})
