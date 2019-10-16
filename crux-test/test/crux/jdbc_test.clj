(ns crux.jdbc-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures.api :as apif :refer [*api*]]
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.lubm :as fl]
            [crux.fixtures.postgres :as fp]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]))

(defn- with-each-jdbc-node [f]
  (t/testing "H2 Database"
    (fj/with-jdbc-node "h2" f))
  (t/testing "SQLite Database"
    (fj/with-jdbc-node "sqlite" f))
  (t/testing "Postgresql Database"
    (fp/with-embedded-postgres f))

  ;; Optional:
  (when (.exists (clojure.java.io/file ".testing-mysql.edn"))
    (t/testing "MYSQL Database"
      (fj/with-jdbc-node "mysql" f
        (read-string (slurp ".testing-mysql.edn")))))
  (when (.exists (clojure.java.io/file ".testing-oracle.edn"))
    (t/testing "Oracle Database"
      (fj/with-jdbc-node "oracle" f
        (read-string (slurp ".testing-oracle.edn"))))))

(t/use-fixtures :each with-each-jdbc-node kvf/with-kv-dir apif/with-node)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) :origin-man))
    (t/testing "Tx log"
      (with-open [tx-log-context (.newTxLogContext *api*)]
        (t/is (= [{:crux.tx/tx-id 2,
                   :crux.tx/tx-time (:crux.tx/tx-time submitted-tx)
                   :crux.tx.event/tx-events
                   [[:crux.tx/put
                     (str (c/new-id (:crux.db/id doc)))
                     (str (c/new-id doc))]]}]
                 (.txLog *api* tx-log-context 0 false)))))))

(defn- docs [dbtype ds id]
  (jdbc/with-transaction [t ds]
    (doall (map (comp (partial j/->v dbtype) :v)
                (jdbc/execute! t ["SELECT V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY = ?" id]
                               {:builder-fn jdbcr/as-unqualified-lower-maps})))))
(t/deftest test-docs-retention
  (let [tx-log (:tx-log *api*)

        doc {:crux.db/id (c/new-id :some-id) :a :b}
        doc-hash (str (c/new-id doc))

        tx-1 (db/submit-tx tx-log [[:crux.tx/put doc]])]

    (t/is (= 1 (count (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))
    (t/is (= [doc] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash)))

    (t/testing "Compaction"
      (db/submit-doc tx-log doc-hash :some-val)
      (t/is (= [doc] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))

    (t/testing "Eviction"
      (db/submit-doc tx-log doc-hash {:crux.db/id :some-id :crux.db/evicted? true})
      (t/is (= [{:crux.db/id :some-id :crux.db/evicted? true}
                {:crux.db/id :some-id :crux.db/evicted? true}] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))

    (t/testing "Resurrect Document"
      (let [tx-2 (db/submit-tx tx-log [[:crux.tx/put doc]])]
        (t/is (= [{:crux.db/id :some-id :crux.db/evicted? true}
                  {:crux.db/id :some-id :crux.db/evicted? true}
                  doc]
                 (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash)))))))

(t/deftest test-micro-bench
  (when (Boolean/parseBoolean (System/getenv "CRUX_JDBC_PERFORMANCE"))
    (let [n 1000
          last-tx (atom nil)]
      (time
       (dotimes [n n]
         (reset! last-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id (keyword (str n))}]]))))

      (time
       (.sync *api* (:crux.tx/tx-time last-tx) nil))))
  (t/is true))

(t/deftest test-ingest-bench
  (when (Boolean/parseBoolean (System/getenv "CRUX_JDBC_PERFORMANCE"))
    (fl/with-lubm-data
      #(t/is (= 1650
                (:num_docs (jdbc/execute-one! (:ds (:tx-log *api*))
                                              ["SELECT count(EVENT_KEY) AS num_docs FROM tx_events WHERE TOPIC = 'docs'"]
                                              {:builder-fn jdbcr/as-unqualified-lower-maps}))))))
  (t/is true))
