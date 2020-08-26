(ns crux.jdbc-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.lubm :as fl]
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
      ;; docker run --rm --name crux-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -p 3306:3306 -d mysql:8.0.21
      ;; mariadb -h 127.0.0.1 -u root -p
      ;; CREATE DATABASE cruxtest;
      (fj/with-mysql-opts {:dbname "cruxtest", :user "root", :password "my-secret-pw"} f))

  #_(when (.exists (clojure.java.io/file ".testing-oracle.edn"))
      (t/testing "Oracle Database"
        (fj/with-jdbc-node "oracle" f
          (read-string (slurp ".testing-oracle.edn")))))

  #_(t/testing "MSSQL Database"
      ;; docker run --rm --name crux-mssql -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=yourStrong(!)Password' -e 'MSSQL_PID=Express' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest-ubuntu
      ;; mssql-cli
      ;; CREATE DATABASE cruxtest;
      (fj/with-mssql-opts {:db-spec {:dbname "cruxtest"}
                           :pool-opts {:username "sa"
                                       :password "yourStrong(!)Password"}}
        f)))

(t/use-fixtures :each with-each-jdbc-node fix/with-node)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.awaitTx *api* submitted-tx (java.time.Duration/ofSeconds 2))
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

(t/deftest test-docs-retention
  (let [doc-store (:document-store *api*)

        doc {:crux.db/id :some-id, :a :b}
        doc-hash (c/new-id doc)

        _ (fix/submit+await-tx [[:crux.tx/put doc]])

        docs (db/fetch-docs doc-store #{doc-hash})]

    (t/is (= 1 (count docs)))
    (t/is (= doc (get docs doc-hash)))

    (t/testing "Compaction"
      (db/submit-docs doc-store [[doc-hash :some-val]])
      (t/is (= :some-val
               (-> (db/fetch-docs doc-store #{doc-hash})
                   (get doc-hash)))))

    (t/testing "Eviction"
      (db/submit-docs doc-store [[doc-hash {:crux.db/id :some-id, :crux.db/evicted? true}]])
      (t/is (nil? (-> (db/fetch-docs doc-store #{doc-hash})
                      (get doc-hash)))))

    (t/testing "Resurrect Document"
      (fix/submit+await-tx [[:crux.tx/put doc]])

      (t/is (= doc
               (-> (db/fetch-docs doc-store #{doc-hash})
                   (get doc-hash)))))))

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
                (:num_docs (jdbc/execute-one! (:pool (:tx-log *api*))
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
