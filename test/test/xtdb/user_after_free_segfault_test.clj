(ns xtdb.user-after-free-segfault-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.io :as xio]))

(defn- kv-node-opts [kv-module]
  {:xtdb/index-store
   {:kv-store {:xtdb/module kv-module,
               :db-dir (xio/create-tmpdir "idx")}},
   :xtdb/document-store
   {:kv-store {:xtdb/module kv-module,
               :db-dir (xio/create-tmpdir "doc")}},
   :xtdb/tx-log
   {:kv-store {:xtdb/module kv-module,
               :db-dir (xio/create-tmpdir "log")}}})

(t/deftest use-after-free-segfault-on-close-1803-rocks-test
  (let [node (xt/start-node (kv-node-opts 'xtdb.rocksdb/->kv-store))]
    (.close node)
    ;; will segfault on early versions
    (t/is (thrown-with-msg? IllegalStateException #"closing" (xt/db node)))
    (t/is (thrown-with-msg? IllegalStateException #"XTDB node is closed" (xt/status node)))
    (t/is (thrown-with-msg? IllegalStateException #"XTDB node is closed" (xt/attribute-stats node)))
    (t/is (thrown-with-msg? IllegalStateException #"TxLog is closed" (xt/latest-submitted-tx node)))
    (t/is (thrown-with-msg? IllegalStateException #"XTDB node is closed" (xt/latest-completed-tx node)))
    (t/is (thrown-with-msg? IllegalStateException #"TxLog is closed" (xt/sync node)))))

(t/deftest use-after-free-segfault-on-close-1803-lmdb-test
  (let [node-opts (kv-node-opts 'xtdb.lmdb/->kv-store)
        node1 (xt/start-node node-opts)]
    (.close node1)
    ;; will segfault on early versions, requires another thread for lmdb for some reason
    ;; bit sporadic, hence the 1000 attempts
    (with-open [node2 (xt/start-node node-opts)]
      (let [exceptions (doall (repeatedly 1000 #(deref (future (try (xt/db node1) nil (catch IllegalStateException e e))))))
            msg-freqs (frequencies (map #(some-> ^Throwable % .getMessage) exceptions))]
        (t/is (= {"closing" 1000} msg-freqs))))))
