(ns ^:no-doc crux.bus
  (:refer-clojure :exclude [send await])
  (:require [crux.io :as cio]
            [crux.system :as sys]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [java.io Closeable]
           [java.util.concurrent ExecutorService Executors TimeUnit]
           [java.time Duration]))

(defprotocol EventSource
  (await [bus opts]
    "opts :: {:crux/event-types, :->result, :timeout, :timeout-value}
     (->result): if it returns a value, we'll return this immediately, otherwise, we'll listen for the events.
     (->result event): if it returns a value, we'll yield that value and close the listener.

     Use ->result to (thread-)safely determine whether to await or immediately return - we won't send through any events while this is determining whether to listen or not.")

  (listen ^java.io.Closeable [bus listen-opts f]))

(alter-meta! #'await assoc :arglists '([bus {:keys [crux/event-types ->result ^Duration timeout timeout-value]}]))
(alter-meta! #'listen assoc :arglists '(^java.io.Closeable
                                        [bus {:crux/keys [event-types], ::keys [executor]} f]))

(defprotocol EventSink
  (send [bus event]))

(s/def :crux/event-type keyword?)

(defmulti event-spec :crux/event-type, :default ::default)
(defmethod event-spec ::default [_] any?)

(s/def ::event (s/merge (s/keys :req [:crux/event-type])
                        (s/multi-spec event-spec :crux/event-type)))

(defn- close-executor [^ExecutorService executor]
  (try
    (.shutdown executor)
    (or (.awaitTermination executor 5 TimeUnit/SECONDS)
        (.shutdownNow executor)
        (log/warn "event bus listener not shut down after 5s"))
    (catch Exception e
      (log/error e "error closing listener"))))

(defrecord EventBus [!listeners ^ExecutorService await-solo-pool sync?]
  EventSource
  (await [this {:keys [crux/event-types ->result ^Duration timeout timeout-value]}]
    (if-let [res (->result) ]
      res ; fast path - don't serialise calls unless we need to
      (let [!latch (promise)]
        (with-open [^java.io.Closeable
                    listener @(.submit await-solo-pool
                                       ^Callable
                                       (fn []
                                         (let [listener (listen this {:crux/event-types event-types
                                                                      ::executor await-solo-pool}
                                                                (fn [evt]
                                                                  (try
                                                                    (when-let [res (->result evt)]
                                                                      (deliver !latch {:res res}))
                                                                    (catch Exception e
                                                                      (deliver !latch {:error e})))))]
                                           (try
                                             (when-let [res (->result)]
                                               (deliver !latch {:res res}))
                                             (catch Exception e
                                               (deliver !latch {:error e})))
                                           listener)))]
          (let [{:keys [res error]} (if timeout
                                      (deref !latch (.toMillis timeout) {:res timeout-value})
                                      (deref !latch))]
            (or res (throw error)))))))

  (listen [this {:crux/keys [event-types], ::keys [executor]} f]
    (let [close-executor? (nil? executor)
          executor (or executor (Executors/newSingleThreadExecutor (cio/thread-factory "bus-listener")))
          listener {:executor executor
                    :f f
                    :crux/event-types event-types}]
      (swap! !listeners (fn [listeners]
                          (reduce (fn [listeners event-type]
                                    (-> listeners (update event-type (fnil conj #{}) listener)))
                                  listeners
                                  event-types)))
      (reify Closeable
        (close [_]
          (swap! !listeners (fn [listeners]
                              (reduce (fn [listeners event-type]
                                        (-> listeners (update event-type disj listener)))
                                      listeners
                                      event-types)))
          (when close-executor?
            (close-executor executor))))))

  EventSink
  (send [_ {:crux/keys [event-type] :as event}]
    (s/assert ::event event)
    (doseq [{:keys [^ExecutorService executor f] :as listener} (get @!listeners event-type)]
      (if sync?
        (try
          (f event)
          (catch Exception e))
        (.submit executor ^Runnable #(f event)))))

  Closeable
  (close [_]
    (close-executor await-solo-pool)

    (doseq [{:keys [executor]} (->> @!listeners (mapcat val))]
      (close-executor executor))))

(defn ->bus {::sys/args {:sync? {:doc "Send bus messages on caller thread"
                                 :default false
                                 :spec ::sys/boolean}}}
  ([] (->bus {}))
  ([{:keys [sync?] :as opts}]
   (->EventBus (atom {})
               (Executors/newSingleThreadExecutor (cio/thread-factory "crux-bus-await-thread"))
               sync?)))
