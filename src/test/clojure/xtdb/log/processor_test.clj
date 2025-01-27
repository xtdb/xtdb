(ns xtdb.log.processor-test
  (:require [clojure.test :as t]
            [xtdb.log :as xt-log]
            [xtdb.log.processor :as proc]
            [xtdb.time :as time])
  (:import [java.nio ByteBuffer]
           java.util.Date))

(t/deftest test-check-chunk-timeout
  (letfn [(check [duration
                  [prev-chunk-tx-id flushed-tx-id]
                  [current-chunk-tx-id latest-completed-tx-id]]
            (when-let [[^ByteBuffer msg-buf new-flush-state]
                       (proc/check-chunk-timeout (time/->instant #inst "2000-01-02") duration
                                                 {:current-chunk-tx-id current-chunk-tx-id,
                                                  :latest-completed-tx-id latest-completed-tx-id}
                                                 {:last-flush-check (time/->instant #inst "2000-01-01")
                                                  :prev-chunk-tx-id prev-chunk-tx-id
                                                  :flushed-tx-id flushed-tx-id})]
              [(= xt-log/hb-flush-chunk (.get msg-buf 0))
               (.getLong msg-buf 1)
               (Date/from (:last-flush-check new-flush-state))
               (:chunk-tx-id new-flush-state)]))]
    (t/is (nil? (check #xt/duration "P2D" [-1 -1] [-1 0]))
          "checked recently, don't check again")

    (t/is (= [true 10 #inst "2000-01-02", 10]
             (check #xt/duration "PT1H" [10 32] [10 40]))
          "we've not flushed recently, we have new txs, submit msg")

    (t/is (nil? (check #xt/duration "PT1H" [10 32] [10 32]))
          "we've not flushed, but no new txs, so don't flush")))
