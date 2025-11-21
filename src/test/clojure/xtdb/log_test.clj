(ns xtdb.log-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util])
  (:import [java.time Instant]
           [xtdb.api.log Log]
           xtdb.arrow.Relation
           [xtdb.util MsgIdUtil]))

(t/use-fixtures :each
  tu/with-allocator
  #_aet/wrap-regen ; uncomment to regenerate expected Arrow EDN files <<no-commit>>
  )

(defn- test-serialize-tx-ops
  ([file tx-ops] (test-serialize-tx-ops file tx-ops {}))
  ([file tx-ops opts]
   (binding [*print-namespace-maps* false]
     (let [file (io/as-file file)
           actual-bytes (log/serialize-tx-ops tu/*allocator*
                                              (for [tx-op tx-ops]
                                                (cond-> tx-op
                                                  (not (record? tx-op)) tx-ops/parse-tx-op))
                                              ;; TODO test multi-db
                                              (merge {:default-db "xtdb"} opts))]


       (with-open [rel (Relation/openFromArrowStream tu/*allocator* actual-bytes)]
         (t/is (= (aet/read-arrow-edn-file file)
                  (doto (aet/->arrow-edn rel)
                    (aet/maybe-write-arrow-edn! file)))
               (str "Mismatch in serialized tx-ops for " (.getName file))))))))

(def devices-docs
  [[:put-docs :device-info
    {:xt/id "device-info-demo000000",
     :api-version "23",
     :manufacturer "iobeam",
     :model "pinto",
     :os-name "6.0.1"}]
   [:put-docs :device-readings
    {:xt/id "reading-demo000000",
     :device-id "device-info-demo000000",
     :cpu-avg-15min 8.654,
     :rssi -50.0,
     :cpu-avg-5min 10.802,
     :battery-status "discharging",
     :ssid "demo-net",
     :time #inst "2016-11-15T12:00:00.000-00:00",
     :battery-level 59.0,
     :bssid "01:02:03:04:05:06",
     :battery-temperature 89.5,
     :cpu-avg-1min 24.81,
     :mem-free 4.10011078E8,
     :mem-used 5.89988922E8}]
   [:put-docs :device-info
    {:xt/id "device-info-demo000001",
     :api-version "23",
     :manufacturer "iobeam",
     :model "mustang",
     :os-name "6.0.1"}]
   [:put-docs :device-readings
    {:xt/id "reading-demo000001",
     :device-id "device-info-demo000001",
     :cpu-avg-15min 8.822,
     :rssi -61.0,
     :cpu-avg-5min 8.106,
     :battery-status "discharging",
     :ssid "stealth-net",
     :time #inst "2016-11-15T12:00:00.000-00:00",
     :battery-level 86.0,
     :bssid "A0:B1:C5:D2:E0:F3",
     :battery-temperature 93.7,
     :cpu-avg-1min 4.93,
     :mem-free 7.20742332E8,
     :mem-used 2.79257668E8}]])

(t/deftest can-write-tx-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-tx.arrow.edn") devices-docs))

(t/deftest can-write-docs-with-different-keys
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/docs-with-different-keys.arrow.edn")
                         [[:put-docs :foo {:xt/id :a, :a 1}]
                          [:put-docs :foo {:xt/id "b", :b 2}]
                          [:put-docs :bar {:xt/id 3, :c 3}]]))

(t/deftest can-write-sql-to-arrow-ipc-streaming-format
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-sql.arrow.edn")
                         [[:sql "INSERT INTO foo (_id) VALUES (0)"]

                          [:sql "INSERT INTO foo (_id, foo, bar) VALUES (?, ?, ?)"
                           [1 nil 3.3]
                           [2 "hello" 12]]

                          [:sql "UPDATE foo FOR PORTION OF VALID_TIME FROM DATE '2021-01-01' TO DATE '2024-01-01' SET bar = 'world' WHERE _id = ?"
                           [1]]

                          [:sql "DELETE FROM foo FOR PORTION OF VALID_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01' WHERE _id = ?"
                           [1]]]))

(t/deftest can-write-opts
  (test-serialize-tx-ops (io/resource "xtdb/tx-log-test/can-write-opts.arrow.edn")
                         [[:sql "INSERT INTO foo (_id) VALUES (0)"]]

                         {:system-time (time/->instant #inst "2021")
                          :default-tz #xt/zone "Europe/London"
                          :authn {:user "xtdb"}}))

(t/deftest validate-offset-returns-proper-errors
  (letfn [(->simulated-log [epoch latest-submitted-offset]
            (reify Log
              (getEpoch [_] epoch)
              (getLatestSubmittedOffset [_] latest-submitted-offset)))

          (->latest-completed-tx [epoch offset]
            (serde/->TxKey (MsgIdUtil/offsetToMsgId epoch offset) (Instant/now)))]

    (t/testing "no error when latestSubmittedOffset >= latestCompletedOffset and same epoch"
      (t/is (nil? (log/validate-offsets (->simulated-log 0 5) (->latest-completed-tx 0 5))))
      (t/is (nil? (log/validate-offsets (->simulated-log 0 10) (->latest-completed-tx 0 5))))
      (t/is (nil? (log/validate-offsets (->simulated-log 1 5) (->latest-completed-tx 1 5)))))

    (t/testing "throws when latestSubmittedOffset < latestCompletedOffset and same epoch - stale log"
      (t/is (thrown-with-msg? IllegalStateException
                              #"Node failed to start due to an invalid transaction log state \(epoch=0, offset=1\)"
                              (log/validate-offsets (->simulated-log 0 1) (->latest-completed-tx 0 2))))
      (t/is (thrown-with-msg? IllegalStateException
                              #"Node failed to start due to an invalid transaction log state \(epoch=1, offset=1\)"
                              (log/validate-offsets (->simulated-log 1 1) (->latest-completed-tx 1 2)))))

    (t/testing "throws when latestSubmittedOffset is -1 and latestCompletedOffset is not for the same epoch - empty log"
      (t/is (thrown-with-msg? IllegalStateException
                              #"Node failed to start due to an invalid transaction log state \(the log is empty\)"
                              (log/validate-offsets (->simulated-log 0 -1) (->latest-completed-tx 0 2))))
      (t/is (thrown-with-msg? IllegalStateException
                              #"Node failed to start due to an invalid transaction log state \(the log is empty\)"
                              (log/validate-offsets (->simulated-log 1 -1) (->latest-completed-tx 1 2)))))

    (t/testing "no error for empty log when epochs differ - validation skipped"
      (t/is (nil? (log/validate-offsets (->simulated-log 1 -1) (->latest-completed-tx 0 2)))))

    (t/testing "no error when latestCompletedTx is nil"
      (t/is (nil? (log/validate-offsets (->simulated-log 0 5) nil))))))

(t/deftest test-memory-log-epochs
  (util/with-tmp-dirs #{local-disk-path}
    ;; Node with local storage and memory log 
    (with-open [node (xtn/start-node {:log [:in-memory {}]
                                       :storage [:local {:path local-disk-path}]})]
      ;; Submit a few transactions
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
      (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
               (set (xt/q node "SELECT _id FROM xt_docs"))))
      ;; Finish the block
      (t/is (nil? (tu/finish-block! node)))

      ;; Submit a few more transactions
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :willbe}]])
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :lost}]])
      (t/is (= (set [{:xt/id :foo}
                     {:xt/id :bar}
                     {:xt/id :willbe}
                     {:xt/id :lost}])
               (set (xt/q node "SELECT _id FROM xt_docs")))))
    
    ;; Node with intact storage and (now) empty memory log 
    (t/is
     (thrown-with-msg?
      IllegalStateException
      #"Node failed to start due to an invalid transaction log state \(the log is empty\)"
      (xtn/start-node {:log [:in-memory {}]
                       :storage [:local {:path local-disk-path}]})))

    ;; Node with intact storage and empty memory log with epoch set to 1
    (with-open [node (xtn/start-node {:log [:in-memory {:epoch 1}]
                                       :storage [:local {:path local-disk-path}]})]
      (t/testing "can query previous indexed values, unindexed values will be lost"
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      (t/testing "can index/query new transactions"
        (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new}]]))
        (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new2}]]))
        (t/is (= (set [{:xt/id :foo}
                       {:xt/id :bar}
                       {:xt/id :new}
                       {:xt/id :new2}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      (t/testing "can finish the block"
        (t/is (nil? (tu/finish-block! node)))))))

(t/deftest test-local-log-epochs
  (util/with-tmp-dirs #{node-dir}
    ;; Node with local storage and local directory log 
    (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                      :storage [:local {:path (.resolve node-dir "objects")}]})]
      ;; Submit a few transactions
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
      (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
               (set (xt/q node "SELECT _id FROM xt_docs"))))
      ;; Finish the block
      (t/is (nil? (tu/finish-block! node)))

      ;; Submit a few more transactions
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :willbe}]])
      (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :lost}]])
      (t/is (= (set [{:xt/id :foo}
                     {:xt/id :bar}
                     {:xt/id :willbe}
                     {:xt/id :lost}])
               (set (xt/q node "SELECT _id FROM xt_docs")))))

    ;; Node with intact storage and empty directory-log
    (t/is
     (thrown-with-msg?
      IllegalStateException
      #"Node failed to start due to an invalid transaction log state \(the log is empty\)"
      (xtn/start-node {:log [:local {:path (.resolve node-dir "new-log")}]
                       :storage [:local {:path (.resolve node-dir "objects")}]})))

    ;; Node with intact storage and empty directory-log with epoch set to 1
    (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "new-log")
                                                    :epoch 1}]
                                       :storage [:local {:path (.resolve node-dir "objects")}]})]
      (t/testing "can query previous indexed values, unindexed values will be lost"
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      (t/testing "can index/query new transactions"
        (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new}]]))
        (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new2}]]))
        (t/is (= (set [{:xt/id :foo}
                       {:xt/id :bar}
                       {:xt/id :new}
                       {:xt/id :new2}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      (t/testing "can finish the block"
        (t/is (nil? (tu/finish-block! node)))))
    
    ;; Restarting the node again with the same new log path and epoch 1
    (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "new-log")
                                                    :epoch 1}]
                                      :storage [:local {:path (.resolve node-dir "objects")}]})]
      (t/testing "can query same transactions + nothing has been re-indexed"
        (t/is (= (set [{:xt/id :foo}
                       {:xt/id :bar}
                       {:xt/id :new}
                       {:xt/id :new2}])
                 (set (xt/q node "SELECT _id FROM xt_docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL")))))

      (t/testing "can index/query new transactions"
        (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new3}]]))
        (t/is (= (set [{:xt/id :foo}
                       {:xt/id :bar}
                       {:xt/id :new}
                       {:xt/id :new2}
                       {:xt/id :new3}])
                 (set (xt/q node "SELECT _id FROM xt_docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL")))))

      (t/testing "can finish another block"
        (t/is (nil? (tu/finish-block! node)))))))

(t/deftest test-local-log-starts-at-correct-point-after-block-cut
  (util/with-tmp-dirs #{node-dir}
    (t/testing "Start a node, write a number of transactions to the log - ensure block is cut"
      (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                        :storage [:local {:path (.resolve node-dir "objects")}]
                                        :indexer {:rows-per-block 20}
                                        :compactor {:threads 0}})]

        (doseq [batch (->> (range 100) (partition-all 10))]
          (xt/execute-tx node (for [i batch] [:put-docs :docs {:xt/id i}])))
        (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
        (t/is (= 10 (count (xt/q node "SELECT * FROM xt.txs"))))
        (t/testing "ensure blocks have been written"
          (Thread/sleep 1000)
          (t/is (= ["l00-rc-b00.arrow" "l00-rc-b01.arrow" "l00-rc-b02.arrow" "l00-rc-b03.arrow" "l00-rc-b04.arrow"]
                   (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))

    (t/testing "Restart the node, ensure it picks up from the correct position in the log"
      (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                        :storage [:local {:path (.resolve node-dir "objects")}]
                                        :indexer {:rows-per-block 20}
                                        :compactor {:threads 0}})]

        (t/testing "shouldn't reindex any transactions when starting up"
          (Thread/sleep 1000)
          (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
          (t/is (= 10 (count (xt/q node "SELECT *  FROM xt.txs")))))

        (t/testing "sending a new transaction shouldnt cut a block yet - still ten blocks off"
          (xt/execute-tx node (for [i (range 101 111)] [:put-docs :docs {:xt/id i}]))
          (t/is (= 110 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
          (t/is (= 11 (count (xt/q node "SELECT *  FROM xt.txs"))))
          (t/is (= ["l00-rc-b00.arrow" "l00-rc-b01.arrow" "l00-rc-b02.arrow" "l00-rc-b03.arrow" "l00-rc-b04.arrow"]
                   (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))))

(t/deftest test-local-log-starts-at-correct-point-after-flush-block
  (util/with-tmp-dirs #{node-dir}
    (t/testing "Start a node, write a number of transactions to the log - ensure block is cut"
      (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                        :storage [:local {:path (.resolve node-dir "objects")}] 
                                        :compactor {:threads 0}})]

        (doseq [batch (->> (range 100) (partition-all 10))]
          (xt/execute-tx node (for [i batch] [:put-docs :docs {:xt/id i}])))
        (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
        (t/is (= 10 (count (xt/q node "SELECT * FROM xt.txs"))))
        (tu/finish-block! node)
        (t/testing "ensure block has been written" 
          (t/is (= ["l00-rc-b00.arrow"] (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))

    (t/testing "Restart the node, ensure it picks up from the correct position in the log"
      (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                        :storage [:local {:path (.resolve node-dir "objects")}]
                                        :compactor {:threads 0}})]

        (t/testing "shouldn't reindex any transactions when starting up"
          (Thread/sleep 1000)
          (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
          (t/is (= 10 (count (xt/q node "SELECT *  FROM xt.txs")))))

        (t/testing "shouldn't have flushed another block / re-read the flush block"
          (t/is (= ["l00-rc-b00.arrow"] (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))))
