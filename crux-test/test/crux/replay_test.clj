(ns crux.replay-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix])
  (:import java.time.Duration))

(t/deftest drop-db
  (fix/with-tmp-dir "event-log-dir" [event-log-dir]
    (fix/with-tmp-dir "db-dir-1" [db-dir-1]
      (with-open [node (crux/start-node {:crux.node/topology '[crux.standalone/topology
                                                               crux.kv.rocksdb/kv-store]
                                         :crux.kv/db-dir (str db-dir-1)
                                         :crux.standalone/event-log-dir (str event-log-dir)
                                         :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})]
        (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :hello}]])))

    (fix/with-tmp-dir "db-dir-2" [db-dir-2]
      (with-open [node (crux/start-node {:crux.node/topology '[crux.standalone/topology
                                                               crux.kv.rocksdb/kv-store]
                                         :crux.kv/db-dir (str db-dir-2)
                                         :crux.standalone/event-log-dir (str event-log-dir)
                                         :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})]
        (t/is (= {:crux.tx/tx-id 0}
                 (crux/latest-submitted-tx node)))
        (t/is (crux/sync node (Duration/ofSeconds 2)))
        (t/is (= {:crux.db/id :hello}
                 (crux/entity (crux/db node) :hello)))))))

(t/deftest test-more-txs
  (let [n 1000]
    (fix/with-tmp-dir "event-log-dir" [event-log-dir]
      (fix/with-tmp-dir "db-dir-1" [db-dir-1]
        (with-open [node (crux/start-node {:crux.node/topology '[crux.standalone/topology
                                                                 crux.kv.rocksdb/kv-store]
                                           :crux.kv/db-dir (str db-dir-1)
                                           :crux.standalone/event-log-dir (str event-log-dir)
                                           :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})]
          (dotimes [x n]
            (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id (str "id-" x)}]]))))

      (fix/with-tmp-dir "db-dir-2" [db-dir-2]
        (with-open [node (crux/start-node {:crux.node/topology '[crux.standalone/topology
                                                                 crux.kv.rocksdb/kv-store]
                                           :crux.kv/db-dir (str db-dir-2)
                                           :crux.standalone/event-log-dir (str event-log-dir)
                                           :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv})]
          (t/is (= {:crux.tx/tx-id (dec n)}
                   (crux/latest-submitted-tx node)))
          (t/is (crux/sync node (Duration/ofSeconds 10)))
          (t/is (= n
                   (count (crux/q (crux/db node) '{:find [?e]
                                                   :where [[?e :crux.db/id]]})))))))))
