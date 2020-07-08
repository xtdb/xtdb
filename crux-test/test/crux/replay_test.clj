(ns crux.replay-test
  (:require [clojure.test :as t]
            [crux.fixtures :refer [*api*] :as fix]
            [crux.fixtures.kafka :as fk]
            crux.jdbc
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.http-server :as fh]
            [crux.api :as crux]
            [crux.fixtures :as f]
            [clojure.java.io :as io])
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
