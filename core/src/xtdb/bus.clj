(ns ^:no-doc xtdb.bus
  (:refer-clojure :exclude [send await])
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import java.io.Closeable
           java.util.function.BiConsumer
           [java.util.concurrent CompletableFuture Executor Executors ExecutorService TimeUnit]))

(defprotocol EventSource
  (await ^java.util.concurrent.CompletableFuture [bus opts]
    "opts :: {:xt/event-types, :->result, :timeout, :timeout-value}
     (->result): if it returns a value, we'll return this immediately, otherwise, we'll listen for the events.
     (->result event): if it returns a value, we'll yield that value and close the listener.

     Use ->result to (thread-)safely determine whether to await or immediately return - we won't send through any events while this is determining whether to listen or not.")

  (listen ^java.io.Closeable [bus listen-opts f]))

(alter-meta! #'await assoc :arglists '(^java.util.concurrent.CompletableFuture
                                       [bus {:keys [xt/event-types ->result]}]))

(alter-meta! #'listen assoc :arglists '(^java.io.Closeable
                                        [bus {:xt/keys [event-types], ::keys [executor]} f]))

(defprotocol EventSink
  (send [bus event]))

(s/def :xt/event-type keyword?)

(defmulti event-spec :xt/event-type, :default ::default)
(defmethod event-spec ::default [_] any?)

(s/def ::event (s/merge (s/keys :req [:xt/event-type])
                        (s/multi-spec event-spec :xt/event-type)))

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
  (await [this {:keys [xt/event-types ->result]}]
    (if-let [res (->result)]
      (CompletableFuture/completedFuture res) ; fast path - don't serialise calls unless we need to
      (let [fut (CompletableFuture.)
            ^java.io.Closeable listener @(.submit await-solo-pool
                                                  ^Callable
                                                  (fn []
                                                    (let [listener (listen this {:xt/event-types event-types
                                                                                 ::executor await-solo-pool}
                                                                           (fn [evt]
                                                                             (try
                                                                               (when-let [res (->result evt)]
                                                                                 (.complete fut res))
                                                                               (catch Exception e
                                                                                 (.completeExceptionally fut e)))))]
                                                      (try
                                                        (when-let [res (->result)]
                                                          (.complete fut res))
                                                        (catch Exception e
                                                          (.completeExceptionally fut e)))
                                                      listener)))]
        (-> fut
            (.whenComplete (reify BiConsumer
                             (accept [_ res e]
                               (.close listener))))))))

  (listen [_ {:xt/keys [event-types], ::keys [executor]} f]
    (let [close-executor? (nil? executor)
          executor (or executor (Executors/newSingleThreadExecutor (xio/thread-factory "bus-listener")))
          listener {:executor executor
                    :f f
                    :xt/event-types event-types
                    :close-executor? close-executor?}]
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
  (send [_ {:xt/keys [event-type] :as event}]
    (s/assert ::event event)
    (doseq [{:keys [^Executor executor f]} (get @!listeners event-type)]
      (try
        (if sync?
          (f event)
          (.execute executor ^Runnable #(f event)))
        (catch Exception _))))

  Closeable
  (close [_]
    (close-executor await-solo-pool)

    (doseq [{:keys [executor close-executor?]} (->> @!listeners (mapcat val))]
      (when close-executor?
        (close-executor executor)))))

(defn ->bus {::sys/args {:sync? {:doc "Send bus messages on caller thread"
                                 :default false
                                 :spec ::sys/boolean}}}
  ([] (->bus {}))
  ([{:keys [sync?]}]
   (->EventBus (atom {})
               (Executors/newSingleThreadExecutor (xio/thread-factory "crux-bus-await-thread"))
               sync?)))
