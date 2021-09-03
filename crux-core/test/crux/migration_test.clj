(ns crux.migration-test
  (:require [crux.api :as crux]
            [crux.codec :as c]
            [crux.fixtures :as fix]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import java.io.File))

(def migration-test-nodes-dir
  (io/file (-> (io/as-file (io/resource "crux/migration_test.clj"))
               (.getParentFile) (.getParentFile) (.getParentFile))
           "test-resources" "crux" "migration_test"))

(defn ->node-opts [^File node-dir]
  {:crux/tx-log {:kv-store {:db-dir (io/file node-dir "txs")}}
   :crux/document-store {:kv-store {:db-dir (io/file node-dir "docs")}}
   :crux/index-store {:kv-store {:db-dir (io/file node-dir "idxs")}}})

(defn with-migration-test-node [node-dir-name index-version build-node-f test-node-f]
  (let [node-dir (io/file migration-test-nodes-dir node-dir-name)]
    (when-not (.exists node-dir)
      (with-open [node (crux/start-node (->node-opts node-dir))]
        (build-node-f node)
        (crux/sync node)))

    (fix/with-tmp-dirs #{copy-dir}
      (fs/copy-dir-into node-dir copy-dir)

      (with-open [node (crux/start-node (cond-> (->node-opts copy-dir)
                                          (not= index-version c/index-version) (dissoc :crux/index-store)))]
        (crux/sync node)
        (test-node-f node)))))

(t/deftest test-basic-node
  (with-migration-test-node "test-basic-node" 18
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :foo}]]))

    (fn [node]
      (let [db (crux/db node)]
        (t/is (= {:crux.db/id :foo}
                 (crux/entity db :foo)))

        (t/is (= #{[:foo]}
                 (crux/q db '{:find [?eid]
                              :where [[?eid :crux.db/id :foo]]})))

        (t/is (= #{[{:crux.db/id :foo}]}
                 (crux/q db '{:find [(pull ?eid [*])]
                              :where [[?eid :crux.db/id :foo]]})))))))

(t/deftest test-match-evict
  (with-migration-test-node "test-match-evict" 18
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :foo}]])
      (fix/submit+await-tx node [[:crux.tx/match {:crux.db/id :foo}]
                                 [:crux.tx/put {:crux.db/id :yes}]])
      (fix/submit+await-tx node [[:crux.tx/match :foo nil]
                                 [:crux.tx/put {:crux.db/id :no}]])
      (fix/submit+await-tx node [[:crux.tx/evict :foo]]))

    (fn [node]
      (let [db (crux/db node)]
        (t/is (nil? (crux/entity db :foo)))

        (t/is (= #{[:yes]}
                 (crux/q db '{:find [?eid]
                              :where [[?eid :crux.db/id]]})))))))

(t/deftest test-tx-fn
  (with-migration-test-node "test-tx-fn" 18
    (fn [node]
      (fix/submit+await-tx node [[:crux.tx/put {:crux.db/id :the-fn
                                                :crux.db/fn '(fn [ctx ops] ops)}]])
      (fix/submit+await-tx node [[:crux.tx/fn :the-fn [[:crux.tx/put {:crux.db/id :foo}]]]]))

    (fn [node]
      (let [db (crux/db node)]
        (t/is (= {:crux.db/id :foo}
                 (crux/entity db :foo)))))))
