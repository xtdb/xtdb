(ns xtdb.indexer.crash-logger-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.time Instant InstantSource ZoneId)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.arrow NullVector Relation)
           (xtdb.indexer CrashLogger)
           (xtdb.trie MemoryHashTrie)))

(t/deftest test-crash-log
  (let [node-dir (util/->path "target/crash-logger-test/test-crash-log")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :instant-src (tu/->mock-clock)})]
      (xt/execute-tx node [[:put-docs :foo {:xt/id 2}]])

      (let [node-id "xtdb-foo-node"
            ^BufferAllocator al (.getAllocator node)
            db (db/primary-db node)
            bp (.getBufferPool db)
            live-idx (.getLiveIndex db)
            crash-logger (CrashLogger. al bp node-id (InstantSource/fixed Instant/EPOCH))
            tx-ops [[:put-docs :public/foo {:xt/id 3, :version 0}]
                    [:put-docs :public/foo {:xt/id 4, :version 0}]]]

        (with-open [live-idx-tx (.startTx live-idx (serde/->TxKey 1 Instant/EPOCH))
                    query-rel (Relation/openFromRows al [{:foo "bar", :baz 32}, {:foo "baz", :baz 64}])
                    tx-ops-rel (Relation/openFromArrowStream al (xt-log/serialize-tx-ops al
                                                                                         (mapv tx-ops/parse-tx-op tx-ops)
                                                                                         {:default-db "xtdb"
                                                                                          :system-time #xt/zdt "1970-01-01T00:00Z[UTC]"
                                                                                          :default-tz (ZoneId/of "Europe/London")}))]
          (let [live-table-tx (.liveTable live-idx-tx #xt/table foo)
                tx-ops-rdr (.getListElements (.vectorFor tx-ops-rel "tx-ops"))]
            (.logPut live-table-tx (ByteBuffer/allocate 16) 0 0
                     (fn []
                       (.writeObject (.getDocWriter live-table-tx) {:xt/id 3, :version 0})))
            (.writeCrashLog crash-logger
                            (pr-str {:table #xt/table foo, :foo "bar", :ex "test crash log"})
                            #xt/table foo live-idx live-table-tx
                            query-rel tx-ops-rdr)))

          (t/is (= {:table #xt/table foo, :foo "bar", :ex "test crash log"}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/crash.edn" node-id))]
                     (-> (.getByteArray bp path)
                         String.
                         read-string))))

          (t/is (= {:tree [:leaf [0]], :page-limit 1024, :log-limit 64}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-trie.binpb" node-id))]
                     (trie/<-MemoryHashTrie (MemoryHashTrie/fromProto (.getByteArray bp path) (NullVector. "_iid" true 0))))))

          (t/is (= {:tree [:leaf [0]], :page-limit 1024, :log-limit 64}
                   (let [path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-trie-tx.binpb" node-id))]
                     (trie/<-MemoryHashTrie (MemoryHashTrie/fromProto (.getByteArray bp path) (NullVector. "_iid" true 0))))))

          ;; Committed live-table data
          (let [live-table-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-table.arrow" node-id))
                footer (.getFooter bp live-table-path)]
            (with-open [rb (.getRecordBatchSync bp live-table-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [{:xt/system-from (time/->zdt #inst "2020"),
                         :xt/valid-from (time/->zdt #inst "2020"),
                         :xt/valid-to (time/->zdt time/end-of-time)
                         :op #xt/tagged [:put {:xt/id 2}]}]
                       (->> (.getAsMaps rel)
                            (mapv #(dissoc % :xt/iid)))))))

          ;; Transaction-scoped data
          (let [live-table-tx-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/live-table-tx.arrow" node-id))
                footer (.getFooter bp live-table-tx-path)]
            (with-open [rb (.getRecordBatchSync bp live-table-tx-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [{:xt/system-from #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :xt/valid-from #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :xt/valid-to #xt/zdt "1970-01-01T00:00Z[UTC]",
                         :op #xt/tagged [:put {:xt/id 3, :version 0}]}]
                       (->> (.getAsMaps rel)
                            (mapv #(dissoc % :xt/iid)))))))

          (let [query-rel-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/query-rel.arrow" node-id))
                footer (.getFooter bp query-rel-path)]
            (with-open [rb (.getRecordBatchSync bp query-rel-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [{:foo "bar", :baz 32}, {:foo "baz", :baz 64}]
                       (->> (.getAsMaps rel)
                            (mapv #(dissoc % :xt/iid)))))))

          (let [tx-ops-path (util/->path (format "crashes/%s/1970-01-01T00:00:00Z/tx-ops.arrow" node-id))
                footer (.getFooter bp tx-ops-path)]
            (with-open [rb (.getRecordBatchSync bp tx-ops-path 0)
                        rel (Relation/fromRecordBatch al (.getSchema footer) rb)]
              (t/is (= [[{"_id" 3, "version" 0}] [{"_id" 4, "version" 0}]]
                       (-> (.vectorFor rel "$data$")
                           (.vectorFor "put-docs")
                           (.vectorFor "documents")
                           (.vectorFor "public/foo")
                           (.getAsList))))))))))
