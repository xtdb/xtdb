(ns xtdb.tx.subscribe
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio])
  (:import xtdb.api.ICursor
           java.time.Duration
           [java.util.concurrent CompletableFuture Semaphore]
           java.util.function.BiConsumer))

(def ^:private ^:const tx-batch-size 50)

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (xio/thread-factory "xtdb-tx-subscription"))

(defn completable-thread [f]
  (let [fut (CompletableFuture.)
        thread (doto (.newThread subscription-thread-factory
                                 (fn []
                                   (try
                                     (f fut)
                                     (.complete fut nil)
                                     (catch Throwable t
                                       (.completeExceptionally fut t)))))
                 (.start))]
    (doto fut
      (.whenComplete (reify BiConsumer
                       (accept [_ _v e]
                         (when-not (instance? InterruptedException e)
                           (.interrupt thread))))))))

(defn- tx-handler [f ^CompletableFuture fut]
  (fn [_last-tx-id txs]
    (when (Thread/interrupted)
      (throw (InterruptedException.)))

    (when (.isDone fut)
      (reduced nil))

    (f fut txs)

    (::xt/tx-id (last txs))))

(defn handle-polling-subscription [tx-log after-tx-id {:keys [^Duration poll-sleep-duration]} f]
  (completable-thread
   (fn [^CompletableFuture fut]
     (loop [after-tx-id after-tx-id]
       (let [last-tx-id (if-let [^ICursor log (try
                                                (db/open-tx-log tx-log after-tx-id)
                                                (catch InterruptedException e (throw e))
                                                (catch Exception e
                                                  (log/warn e "Error polling for txs, will retry")))]
                          (try
                            (reduce (tx-handler f fut)
                                    after-tx-id
                                    (some->> log iterator-seq (partition-all tx-batch-size)))
                            (finally
                              (.close log)))

                          after-tx-id)]
         (cond
           (.isDone fut) nil
           (Thread/interrupted) (throw (InterruptedException.))
           :else (do
                   (when (= after-tx-id last-tx-id)
                     (Thread/sleep (.toMillis poll-sleep-duration)))
                   (recur last-tx-id))))))))

(defprotocol PNotifyingSubscriberHandler
  (notify-tx! [_ tx])
  (handle-notifying-subscriber [_ tx-log after-tx-id f]))

(defrecord NotifyingSubscriberHandler [!state]
  PNotifyingSubscriberHandler
  (notify-tx! [_ tx]
    (let [{:keys [semaphores]} (swap! !state assoc :latest-submitted-tx-id (::xt/tx-id tx))]
      (doseq [^Semaphore semaphore semaphores]
        (.release semaphore))))

  (handle-notifying-subscriber [_ tx-log after-tx-id f]
    (let [semaphore (Semaphore. 0)
          {:keys [latest-submitted-tx-id]} (swap! !state update :semaphores conj semaphore)]

      (completable-thread
       (fn [^CompletableFuture fut]
         (try
           (let [handle-txs (tx-handler f fut)]
             (loop [after-tx-id after-tx-id]
               (let [last-tx-id (if (and latest-submitted-tx-id
                                         (or (nil? after-tx-id)
                                             (< after-tx-id latest-submitted-tx-id)))

                                  ;; catching up
                                  (with-open [log (db/open-tx-log tx-log after-tx-id)]
                                    (reduce handle-txs
                                            after-tx-id
                                            (->> (iterator-seq log)
                                                 (take-while #(<= (::xt/tx-id %) latest-submitted-tx-id))
                                                 (partition-all tx-batch-size))))

                                  ;; running live
                                  (let [permits (do
                                                  (.acquire semaphore)
                                                  (inc (.drainPermits semaphore)))
                                        limit (if (> permits tx-batch-size)
                                                (do
                                                  (.release semaphore (- permits tx-batch-size))
                                                  tx-batch-size)
                                                permits)]
                                    (with-open [log (db/open-tx-log tx-log after-tx-id)]
                                      (handle-txs after-tx-id
                                                  (->> (iterator-seq log)
                                                       (into [] (take limit)))))))]
                 (cond
                   (.isDone fut) nil
                   (Thread/interrupted) (throw (InterruptedException.))
                   :else (recur last-tx-id)))))

           (finally
             (swap! !state update :semaphores disj semaphore))))))))

(defn ->notifying-subscriber-handler [latest-submitted-tx]
  (->NotifyingSubscriberHandler (atom {:latest-submitted-tx-id (::xt/tx-id latest-submitted-tx)
                                       :semaphores #{}})))
