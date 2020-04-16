(ns ^:no-doc crux.bus
  (:refer-clojure :exclude [send])
  (:require [crux.io :as cio]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [java.io Closeable]
           [java.util.concurrent ExecutorService Executors TimeUnit]))

(defprotocol EventSource
  (listen [_ f] [_ listen-ops f]))

(defprotocol EventSink
  (send [_ event]))

(s/def ::event-type keyword?)
(s/def ::event-types (s/coll-of ::event-type :kind set?))

(defmulti event-spec ::event-type, :default ::default)
(defmethod event-spec ::default [_] any?)

(s/def ::event (s/and (s/keys :req [::event-type])
                      (s/multi-spec event-spec ::event-type)))

(defrecord EventBus [!listeners]
  EventSource
  (listen [this listen-ops f]
    (let [{::keys [event-types]} listen-ops]
      (swap! !listeners
             conj {:executor (Executors/newSingleThreadExecutor (cio/thread-factory "bus-listener"))
                   :f f
                   ::event-types event-types})
      nil))

  (listen [this f]
    (listen this {} f))

  EventSink
  (send [_ {::keys [event-type] :as event}]
    (s/assert ::event event)

    (doseq [{:keys [^ExecutorService executor f ::event-types]} @!listeners]
      (when (or (nil? event-types) (contains? event-types event-type))
        (.submit executor ^Runnable #(f event)))))

  Closeable
  (close [_]
    (doseq [{:keys [^ExecutorService executor]} @!listeners]
      (try
        (.shutdown executor)
        (or (.awaitTermination executor 5 TimeUnit/SECONDS)
            (.shutdownNow executor)
            (log/warn "event bus listener not shut down after 5s"))
        (catch Exception e
          (log/error e "error closing listener"))))))

(def bus
  {:start-fn (fn [deps args]
               (->EventBus (atom #{})))})
