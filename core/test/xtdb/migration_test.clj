(ns xtdb.migration-test
  (:require [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.fixtures :as fix]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import java.io.File))

(def migration-test-nodes-dir
  (io/file (-> (io/as-file (io/resource "xtdb/migration_test.clj"))
               (.getParentFile) (.getParentFile) (.getParentFile))
           "test-resources" "xtdb" "migration_test"))

(defn ->node-opts [^File node-dir]
  {:xtdb/tx-log {:kv-store {:db-dir (io/file node-dir "txs")}}
   :xtdb/document-store {:kv-store {:db-dir (io/file node-dir "docs")}}
   :xtdb/index-store {:kv-store {:db-dir (io/file node-dir "idxs")}}})

(defn with-migration-build-node [node-dir-name build-node-f]
  (let [node-dir (io/file migration-test-nodes-dir node-dir-name)]
    (when-not (.exists node-dir)
      (with-open [node (xt/start-node (->node-opts node-dir))]
        (build-node-f node)
        (xt/sync node)))))

(defn with-migration-test-node [node-dir-name index-version test-node-f]
  (let [node-dir (io/file migration-test-nodes-dir node-dir-name)]
    (fix/with-tmp-dirs #{copy-dir}
      (fs/copy-dir-into node-dir copy-dir)

      (with-open [node (xt/start-node (cond-> (->node-opts copy-dir)
                                        (not= index-version c/index-version) (dissoc :xtdb/index-store)))]
        (xt/sync node)
        (test-node-f node)))))

(t/deftest test-basic-node
  #_ ; uncomment to re-build
  (with-migration-build-node "test-basic-node"
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :foo}]])))

  (with-migration-test-node "test-basic-node" 18
    (fn [node]
      (let [db (xt/db node)]
        (t/is (= {:xt/id :foo}
                 (xt/entity db :foo)))

        (t/is (= #{[:foo]}
                 (xt/q db '{:find [?eid]
                            :where [[?eid :xt/id :foo]]})))

        (t/is (= #{[{:xt/id :foo}]}
                 (xt/q db '{:find [(pull ?eid [*])]
                            :where [[?eid :xt/id :foo]]})))))))

(t/deftest test-match-evict
  #_ ; uncomment to re-build
  (with-migration-build-node "test-match-evict"
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :foo}]])
      (fix/submit+await-tx node [[:crux.tx/match {:crux.db/id :foo}]
                                 [:crux.tx/put {:crux.db/id :yes}]])
      (fix/submit+await-tx node [[:crux.tx/match :foo nil]
                                 [:crux.tx/put {:crux.db/id :no}]])
      (fix/submit+await-tx node [[:crux.tx/evict :foo]])))

  (with-migration-test-node "test-match-evict" 18
    (fn [node]
      (let [db (xt/db node)]
        (t/is (nil? (xt/entity db :foo)))

        (t/is (= #{[:yes]}
                 (xt/q db '{:find [?eid]
                            :where [[?eid :xt/id]]})))))))

(t/deftest test-tx-fn
  #_ ; uncomment to re-build
  (with-migration-build-node "test-tx-fn"
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :the-fn
                                                :crux.db/fn '(fn [ctx ops] ops)}]])
      (fix/submit+await-tx node [[:crux.tx/fn :the-fn [[:crux.tx/put {:crux.db/id :foo}]]]])))

  (t/testing "we try to call the function without re-inserting it - this breaks"
    (t/is (thrown-with-msg? IllegalStateException
                            #"^Legacy Crux tx-fn"
                            (try
                              (with-migration-test-node "test-tx-fn" 18
                                (fn [node]
                                  (fix/submit+await-tx node [[::xt/fn :the-fn [[::xt/put {:xt/id :bar}]]]])))
                              (catch Exception e
                                (throw (or (.getCause e) e)))))))

  (t/testing "once we add an `::xt/fn` implementation, we can continue"
    (with-migration-test-node "test-tx-fn" 18
      (fn [node]
        (fix/submit+await-tx node [[::xt/put {:xt/id :the-fn
                                              :crux.db/fn '(fn [ctx ops] ops)
                                              :xt/fn '(fn [ctx ops] ops)}]])
        (fix/submit+await-tx node [[::xt/fn :the-fn [[::xt/put {:xt/id :bar}]]]])
        (let [db (xt/db node)]
          (t/is (= {:xt/id :foo}
                   (xt/entity db :foo))))))))
