(ns crux.tx.subscribe
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.tx :as tx])
  (:import crux.api.ICursor
           java.time.Duration
           [java.util.concurrent CompletableFuture Semaphore]
           java.util.function.BiConsumer))

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (cio/thread-factory "crux-tx-subscription"))

(defn- completable-thread [f]
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
                       (accept [_ v e]
                         (when-not (instance? InterruptedException e)
                           (.interrupt thread))))))))

(defn- tx-handler [f ^CompletableFuture fut]
  (fn [_last-tx-id tx]
    (when (Thread/interrupted)
      (throw (InterruptedException.)))

    (when (.isDone fut)
      (reduced nil))

    (f fut tx)

    (::tx/tx-id tx)))

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
                                    (some-> log iterator-seq))
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
    (let [{:keys [semaphores]} (swap! !state assoc :latest-submitted-tx-id (::tx/tx-id tx))]
      (doseq [^Semaphore semaphore semaphores]
        (.release semaphore))))

  (handle-notifying-subscriber [_ tx-log after-tx-id f]
    (let [semaphore (Semaphore. 0)
          {:keys [latest-submitted-tx-id]} (swap! !state update :semaphores conj semaphore)]

      (completable-thread
       (fn [^CompletableFuture fut]
         (try
           (loop [after-tx-id after-tx-id]
             (let [last-tx-id (if (and latest-submitted-tx-id
                                       (or (nil? after-tx-id)
                                           (< after-tx-id latest-submitted-tx-id)))

                                ;; catching up
                                (with-open [log (db/open-tx-log tx-log after-tx-id)]
                                  (reduce (tx-handler f fut)
                                          after-tx-id
                                          (->> (iterator-seq log)
                                               (take-while #(<= (::tx/tx-id %) latest-submitted-tx-id)))))

                                ;; running live
                                (reduce (tx-handler f fut)
                                        after-tx-id
                                        (let [permits (do
                                                        (.acquire semaphore)
                                                        (inc (.drainPermits semaphore)))
                                              limit (if (> permits 100)
                                                      (do
                                                        (.release semaphore (- permits 100))
                                                        100)
                                                      permits)]
                                          (with-open [log (db/open-tx-log tx-log after-tx-id)]
                                            (->> (iterator-seq log)
                                                 (into [] (take limit)))))))]
               (cond
                 (.isDone fut) nil
                 (Thread/interrupted) (throw (InterruptedException.))
                 :else (recur last-tx-id))))

           (finally
             (swap! !state update :semaphores disj semaphore))))))))

(defn ->notifying-subscriber-handler [latest-submitted-tx]
  (->NotifyingSubscriberHandler (atom {:latest-submitted-tx-id (::tx/tx-id latest-submitted-tx)
                                       :semaphores #{}})))
