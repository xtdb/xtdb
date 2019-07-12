(ns crux.jdbc.jdbc-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.jdbc :as j]
            [taoensso.nippy :as nippy]
            [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.jdbc.fixtures.jdbc :as fj]
            [crux.jdbc.fixtures.postgres :as fp]
            [crux.codec :as c]
            [crux.kafka :as k])
  (:import crux.api.ICruxAPI))

(defn- with-each-jdbc-system [f]
  (t/testing "H2 Database"
    (fj/with-jdbc-system f))
  (t/testing "Postgresql Database"
    (fp/with-embedded-postgres f)))

(t/use-fixtures :each with-each-jdbc-system)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) :origin-man))))

(defn- docs [ds id]
  (map (comp nippy/thaw :TX_EVENTS/V) (jdbc/execute! ds ["SELECT V FROM TX_EVENTS WHERE TOPIC = 'doc' AND EVENT_KEY = ?" id])))

(t/deftest test-docs-retention
  (let [tx-log (:tx-log *api*)

        doc {:crux.db/id (c/new-id :some-id) :a :b}
        doc-hash (str (c/new-id doc))

        tx-1 (db/submit-tx tx-log [[:crux.tx/put doc]])]

    (t/is (= 1 (count (docs (:ds (:tx-log *api*)) doc-hash))))
    (t/is (= [doc] (docs (:ds (:tx-log *api*)) doc-hash)))

    (t/testing "Compaction"
      (db/submit-doc tx-log doc-hash {:crux.db/id (c/new-id :some-id) :a :evicted})
      (t/is (= [{:crux.db/id (c/new-id :some-id) :a :evicted}] (docs (:ds (:tx-log *api*)) doc-hash))))))
