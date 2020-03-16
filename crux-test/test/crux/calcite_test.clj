(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.api :as api]
            [crux.fixtures.standalone :as fs]
            [crux.calcite :as cal]
            [crux.kv :as kv]
            [crux.node :as n]
            [crux.fixtures :as f])
  (:import java.sql.DriverManager
           crux.calcite.CruxSchemaFactory))

;; How to wire this all in?

;; https://github.com/juxt/crux/issues/514
;; Aggregations, Joins
;; https://calcite.apache.org/docs/cassandra_adapter.html

;; What is a table? (list of columns & also a grouping of documents (i.e. mongo collections))
;; What do joins mean
;; Table could be a datalog rule
;; Inner maps are ? ignored

(def ^:dynamic ^java.sql.Connection *conn*)
(defn- with-jdbc-connection [f]
  ;;
  ;;
  ;; (DriverManager/getConnection "jdbc:calcite:model=crux-calcite/resources/model.json")

  (with-open [conn (DriverManager/getConnection "jdbc:avatica:remote:url=http://localhost:1501;serialization=protobuf")]
    (binding [*conn* conn]
      (f))))

(defn with-calcite-module [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cal/module))
    f))

(defn- query [q]
  (let [stmt (.createStatement *conn*)]
    (->> q (.executeQuery stmt) resultset-seq)))

(t/use-fixtures :each fs/with-standalone-node with-calcite-module kvf/with-kv-dir fapi/with-node with-jdbc-connection)

(t/deftest test-hello-world-query
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth"}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars"}]))

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name "Ivan"]
                                                         [e :name name]]}))))
  (t/testing "retrieve data"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON"))))
  (t/testing "multiple columns"
    (t/is (= [{:name "Ivan" :homeworld "Earth"}
              {:name "Malcolm" :homeworld "Mars"}]
             (query "SELECT PERSON.NAME,PERSON.HOMEWORLD FROM PERSON"))))
  ;; TODO Broken for various reasons:
  (t/testing "wildcard columns"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}
               {:name "Malcolm" :homeworld "Mars" :id ":malcolm"}}
             (set (query "SELECT * FROM PERSON")))))
  (t/testing "unknown column"
    (t/is (thrown-with-msg? java.sql.SQLException #"Column 'NOCNOLUMN' not found in any table"
                            (query "SELECT NOCNOLUMN FROM PERSON")))))

;; So how we gonna do table?
;; Store as document #strategy one, table {}
;; Generate from query? (get the mechanism working first)
;; Probably easier to put into context

#_(t/deftest test-ordering
  (f/transact! *api* (f/people [{:crux.db/id :ivan :age 1}
                                {:crux.db/id :petr :age 2}]))

  (t/is (= [{:id ":ivan"}
            {:id ":malcolm"}]
           (query "SELECT PERSON.NAME FROM PERSON ORDER BY AGE"))))

;; Mongo leverage this concept of collections:
   ;; for (String collectionName : mongoDb.listCollectionNames()) {
   ;;    builder.put(collectionName, new MongoTable(collectionName));
   ;;  }
