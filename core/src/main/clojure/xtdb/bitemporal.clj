(ns xtdb.bitemporal
  (:require [xtdb.util :as util])
  (:import [java.util LinkedList List]))

;; As the algorithm processes events in reverse system time order, one can
;; immediately write out the system-to times when having finished an event.
;; The system-to times are not relevant for processing earlier events.
(defrecord Rectangle [^long valid-from, ^long valid-to, ^long sys-from])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface RowConsumer
  (^void accept [^int idx, ^long validFrom, ^long validTo, ^long systemFrom, ^long systemTo]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface EventResolver
  (^void resolveEvent [^int idx, ^long validFrom, ^long validTo, ^long systemFrom
                       ^xtdb.bitemporal.RowConsumer rowConsumer])
  (^void nextIid []))

(defn event-resolver
  (^xtdb.bitemporal.EventResolver [] (event-resolver (LinkedList.)))

  (^xtdb.bitemporal.EventResolver [^List !ranges]
   (reify EventResolver
     (nextIid [_]
       (.clear !ranges))

     ;; https://en.wikipedia.org/wiki/Allen%27s_interval_algebra
     (resolveEvent [_ idx valid-from valid-to system-from rc]
       (when rc
         (let [itr (.iterator !ranges)]
           (loop [valid-from valid-from]
             (when (< valid-from valid-to)
               (if-not (.hasNext itr)
                 (.accept rc idx valid-from valid-to system-from util/end-of-time-μs)

                 (let [^Rectangle r (.next itr)]
                   (if (<= (.valid-to r) valid-from)
                     ;; state #{< m} event
                     (recur valid-from)

                     ;; state #{> mi o oi s si d di f fi =} event
                     (do
                       (when (< valid-from (.valid-from r))
                         ;; state #{> mi oi d f} event
                         (let [valid-to (min valid-to (.valid-from r))]
                           (when (< valid-from valid-to)
                             (.accept rc idx valid-from valid-to
                                      system-from util/end-of-time-μs))))

                       (let [valid-from (max valid-from (.valid-from r))
                             valid-to (min valid-to (.valid-to r))]
                         (when (< valid-from valid-to)
                           (.accept rc idx valid-from valid-to system-from (.sys-from r))))

                       (when (< (.valid-to r) valid-to)
                         ;; state #{o s d} event
                         (recur (.valid-to r)))))))))))

       (let [itr (.listIterator !ranges)]
         (loop [ev-added? false]
           (let [^Rectangle r (when (.hasNext itr)
                                (.next itr))]
             (cond
               (nil? r) (when-not ev-added?
                          (.add itr (Rectangle. valid-from valid-to system-from)))

               ;; state #{< m} event
               (<= (.valid-to r) valid-from) (recur ev-added?)

               ;; state #{> mi o oi s si d di f fi =} event
               :else (do
                       (if (< (.valid-from r) valid-from)
                         ;; state #{o di fi} event
                         (.set itr (Rectangle. (.valid-from r) valid-from (.sys-from r)))

                         ;; state #{> mi oi s si d f =} event
                         (.remove itr))

                       (when-not ev-added?
                         (.add itr (Rectangle. valid-from valid-to system-from)))

                       (when (< valid-to (.valid-to r))
                         ;; state #{> mi oi si di}
                         (let [valid-from (max valid-to (.valid-from r))]
                           (when (< valid-from (.valid-to r))
                             (.add itr (Rectangle. valid-from (.valid-to r) (.sys-from r))))))

                       (recur true)))))

         #_ ; asserting the ranges invariant isn't ideal on the fast path
         (assert (->> (partition 2 1 !ranges)
                      (every? (fn [[^Rectangle r1 ^Rectangle r2]]
                                (<= (.valid-to r1) (.valid-from r2)))))
                 {:ranges (mapv (juxt (comp util/micros->instant :valid-from)
                                      (comp util/micros->instant :valid-to)
                                      (comp util/micros->instant :sys-from))
                                !ranges)
                  :ev [(util/micros->instant valid-from)
                       (util/micros->instant valid-to)
                       (util/micros->instant system-from)]}))))))
