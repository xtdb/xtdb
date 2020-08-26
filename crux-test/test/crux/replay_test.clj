(ns crux.replay-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix]
            [crux.rocksdb :as rocks])
  (:import java.time.Duration))

(t/deftest drop-db
  (fix/with-tmp-dir "event-log-dir" [event-log-dir]
    (fix/with-tmp-dir "db-dir-1" [db-dir-1]
      (with-open [node (crux/start-node {:crux/document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                          :db-dir (io/file event-log-dir "doc-store")}}
                                         :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store,
                                                                  :db-dir (io/file event-log-dir "tx-log")}}
                                         :crux/indexer {:kv-store {:crux/module `rocks/->kv-store,
                                                                   :db-dir db-dir-1}}})]
        (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :hello}]])))

    (fix/with-tmp-dir "db-dir-2" [db-dir-2]
      (with-open [node (crux/start-node {:crux/document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                          :db-dir (io/file event-log-dir "doc-store")}}
                                         :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store,
                                                                  :db-dir (io/file event-log-dir "tx-log")}}
                                         :crux/indexer {:kv-store {:crux/module `rocks/->kv-store,
                                                                   :db-dir db-dir-2}}})]
        (t/is (= {:crux.tx/tx-id 0}
                 (crux/latest-submitted-tx node)))
        (t/is (crux/sync node (Duration/ofSeconds 2)))
        (t/is (= {:crux.db/id :hello}
                 (crux/entity (crux/db node) :hello)))))))

(t/deftest test-more-txs
  (let [n 1000]
    (fix/with-tmp-dir "event-log-dir" [event-log-dir]
      (fix/with-tmp-dir "db-dir-1" [db-dir-1]
        (with-open [node (crux/start-node {:crux/document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                            :db-dir (io/file event-log-dir "doc-store")}}
                                           :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store,
                                                                    :db-dir (io/file event-log-dir "tx-log")}}
                                           :crux/indexer {:kv-store {:crux/module `rocks/->kv-store,
                                                                     :db-dir db-dir-1}}})]
          (dotimes [x n]
            (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id (str "id-" x)}]]))))

      (fix/with-tmp-dir "db-dir-2" [db-dir-2]
        (with-open [node (crux/start-node {:crux/document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                            :db-dir (io/file event-log-dir "doc-store")}}
                                           :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store,
                                                                    :db-dir (io/file event-log-dir "tx-log")}}
                                           :crux/indexer {:kv-store {:crux/module `rocks/->kv-store,
                                                                     :db-dir db-dir-2}}})]
          (t/is (= {:crux.tx/tx-id (dec n)}
                   (crux/latest-submitted-tx node)))
          (t/is (crux/sync node (Duration/ofSeconds 10)))
          (t/is (= n
                   (count (crux/q (crux/db node) '{:find [?e]
                                                   :where [[?e :crux.db/id]]})))))))))
