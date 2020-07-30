(ns crux.jdbc-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.lubm :as fl]
            [crux.api :as api]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]))

(defn- with-each-jdbc-node [f]
  (t/testing "H2 Database"
    (fj/with-h2-opts f))
  (t/testing "SQLite Database"
    (fj/with-sqlite-opts f))
  (t/testing "Postgresql Database"
    (fj/with-embedded-postgres f))

  ;; Optional:
  #_(t/testing "MySQL Database"
      ;; docker run --name crux-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -p 3306:3306 -d mysql:8.0.21
      (fj/with-mysql-opts {:dbname "cruxtest", :user "root", :password "my-secret-pw"} f))

  #_(when (.exists (clojure.java.io/file ".testing-oracle.edn"))
      (t/testing "Oracle Database"
        (fj/with-jdbc-node "oracle" f
          (read-string (slurp ".testing-oracle.edn")))))

  #_(t/testing "MSSQL Database"
      ;; docker run --name crux-mssql -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=yourStrong(!)Password' -e 'MSSQL_PID=Express' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest-ubuntu
      ;; Then create the DB (use mssql-cli).
      (fj/with-mssql-opts {:dbname "cruxtest", :user "sa", :password "yourStrong(!)Password"} f)))

(t/use-fixtures :each with-each-jdbc-node fix/with-node)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.awaitTx *api* submitted-tx nil)
    (t/is (.entity (.db *api*) :origin-man))
    (t/testing "Tx log"
      (with-open [tx-log-iterator (.openTxLog *api* 0 false)]
        (t/is (= [{:crux.tx/tx-id 2,
                   :crux.tx/tx-time (:crux.tx/tx-time submitted-tx)
                   :crux.tx.event/tx-events
                   [[:crux.tx/put
                     (c/new-id (:crux.db/id doc))
                     (c/new-id doc)]]}]
                 (iterator-seq tx-log-iterator)))))))

(defn- docs [dbtype ds id]
  (jdbc/with-transaction [t ds]
    (doall (map (comp (partial j/->v dbtype) :v)
                (jdbc/execute! t ["SELECT V FROM tx_events WHERE TOPIC = 'docs' AND EVENT_KEY = ?" id]
                               {:builder-fn jdbcr/as-unqualified-lower-maps})))))
(t/deftest test-docs-retention
  (let [tx-log (:tx-log *api*)
        doc-store (:document-store *api*)

        doc {:crux.db/id (c/new-id :some-id) :a :b}
        doc-hash (str (c/new-id doc))

        tx-1 (api/submit-tx *api* [[:crux.tx/put doc]])]

    (t/is (= 1 (count (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))
    (t/is (= [doc] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash)))

    (t/testing "Compaction"
      (db/submit-docs doc-store [[doc-hash :some-val]])
      (t/is (= [:some-val] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))

    (t/testing "Eviction"
      (db/submit-docs doc-store [[doc-hash {:crux.db/id :some-id :crux.db/evicted? true}]])
      (t/is (= [{:crux.db/id :some-id :crux.db/evicted? true}
                {:crux.db/id :some-id :crux.db/evicted? true}] (docs fj/*dbtype* (:ds (:tx-log *api*)) doc-hash))))

    (t/testing "Resurrect Document"
      (let [tx-2 (api/submit-tx *api* [[:crux.tx/put doc]])]
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
       (.awaitTx *api* last-tx nil))))
  (t/is true))

(t/deftest test-ingest-bench
  (when (Boolean/parseBoolean (System/getenv "CRUX_JDBC_PERFORMANCE"))
    (fl/with-lubm-data
      #(t/is (= 1650
                (:num_docs (jdbc/execute-one! (:ds (:tx-log *api*))
                                              ["SELECT count(EVENT_KEY) AS num_docs FROM tx_events WHERE TOPIC = 'docs'"]
                                              {:builder-fn jdbcr/as-unqualified-lower-maps}))))))
  (t/is true))

(t/deftest test-project-star-bug-1016
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put
                                       :crux.db/fn '(fn [ctx doc]
                                                      [[:crux.tx/put doc]])}]])
  (fix/submit+await-tx [[:crux.tx/fn :put {:crux.db/id :foo, :foo :bar}]])

  (let [db (api/db *api*)]

    (t/is (= #{[{:crux.db/id :foo, :foo :bar}]}
             (api/q db
                    '{:find [(eql/project ?e [*])]
                      :where [[?e :crux.db/id :foo]]})))

    (t/is (= {:crux.db/id :foo, :foo :bar}
             (api/entity db :foo)))

    (t/is (= #{[{:crux.db/id :foo, :foo :bar}]}
             (api/q db
                    '{:find [(eql/project ?e [*])]
                      :where [[?e :crux.db/id :foo]]})))))
