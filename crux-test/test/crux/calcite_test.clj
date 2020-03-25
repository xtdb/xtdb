(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.calcite :as cal]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.node :as n]
            [crux.fixtures.api :as apif])
  (:import java.sql.DriverManager))

;; See https://github.com/juxt/crux/issues/514

(def ^:dynamic ^java.sql.Connection *conn*)

(defn- with-calcite-connection [f]
  (with-open [conn (DriverManager/getConnection "jdbc:calcite:model=crux-calcite/resources/model.json")]
    (binding [*conn* conn]
      (f))))

(defn- with-avatica-connection [f]
  (with-open [conn (DriverManager/getConnection "jdbc:avatica:remote:url=http://localhost:1501;serialization=protobuf")]
    (binding [*conn* conn]
      (f))))

(defn- with-each-connection-type [f]
  (with-calcite-connection f)
  (t/testing "With Avatica Connection"
    (with-avatica-connection f)))

(defn with-calcite-module [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cal/module))
    f))

(defn- query [q]
  (let [stmt (.createStatement *conn*)]
    (->> q (.executeQuery stmt) resultset-seq)))

(t/use-fixtures :each fs/with-standalone-node with-calcite-module kvf/with-kv-dir fapi/with-node with-each-connection-type)

(t/deftest test-hello-world-query
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/columns [{:crux.db/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.db/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.db/attribute :homeworld
                                                 :crux.sql.column/name "homeworld"
                                                 :crux.sql.column/type :varchar}]}])
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth"}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars"}]))

  (t/testing "order by"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC")))
    (t/is (= [{:name "Malcolm"}
              {:name "Ivan"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME DESC"))))

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

  (t/testing "wildcard columns"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}
               {:name "Malcolm" :homeworld "Mars" :id ":malcolm"}}
             (set (query "SELECT * FROM PERSON")))))

  (t/testing "equals operand"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}}
             (set (query "SELECT * FROM PERSON WHERE NAME = 'Ivan'"))))
    (t/is (= #{{:name "Malcolm" :homeworld "Mars" :id ":malcolm"}}
             (set (query "SELECT * FROM PERSON WHERE NAME <> 'Ivan'"))))
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}}
             (set (query "SELECT * FROM PERSON WHERE 'Ivan' = NAME")))))

  (t/testing "in operand"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}}
             (set (query "SELECT * FROM PERSON WHERE NAME in ('Ivan')")))))

  (t/testing "and"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}}
             (set (query "SELECT * FROM PERSON WHERE NAME = 'Ivan' AND HOMEWORLD = 'Earth'")))))

  (t/testing "or"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan"}
               {:name "Malcolm" :homeworld "Mars" :id ":malcolm"}}
             (set (query "SELECT * FROM PERSON WHERE NAME = 'Ivan' OR NAME = 'Malcolm'")))))

  (t/testing "namespaced keywords"
    (f/transact! *api* (f/people [{:crux.db/id :human/ivan :name "Ivan"}]))
    (t/is (= [{:id ":human/ivan", :name "Ivan"}] (query "SELECT ID,NAME FROM PERSON WHERE ID = CRUXID('human/ivan')"))))

  (t/testing "numeric values"
    (apif/submit+await-tx [[:crux.tx/delete :ivan] [:crux.tx/delete :human/ivan] [:crux.tx/delete :malcolm]])
    (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/columns [{:crux.db/attribute :name
                                                   :crux.sql.column/name "name"
                                                   :crux.sql.column/type :varchar}
                                                  {:crux.db/attribute :age
                                                   :crux.sql.column/name "age"
                                                   :crux.sql.column/type :integer}]}])
    (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :age 21}
                                  {:crux.db/id :malcolm :name "Malcolm" :age 25}]))
    (t/is (= [{:name "Ivan" :age 21}]
             (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE = 21")))
    (t/testing "order by"
      (t/is (= [{:name "Ivan"}
                {:name "Malcolm"}]
               (query "SELECT PERSON.NAME FROM PERSON ORDER BY AGE ASC")))
      (t/is (= [{:name "Malcolm"}
                {:name "Ivan"}]
               (query "SELECT PERSON.NAME FROM PERSON ORDER BY AGE DESC")))))

  (t/testing "unknown column"
    (t/is (thrown-with-msg? java.sql.SQLException #"Column 'NOCNOLUMN' not found in any table"
                            (query "SELECT NOCNOLUMN FROM PERSON")))))

(t/deftest test-simple-joins
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/columns [{:crux.db/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.db/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.db/attribute :planet
                                                 :crux.sql.column/name "planet"
                                                 :crux.sql.column/type :varchar}]}
                      {:crux.db/id :crux.sql.schema/planet
                       :crux.sql.table/name "planet"
                       :crux.sql.table/columns [{:crux.db/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.db/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.db/attribute :climate
                                                 :crux.sql.column/name "climate"
                                                 :crux.sql.column/type :varchar}]}])
  (f/transact! *api* (f/people [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                                {:crux.db/id :planet/earth :name "earth" :climate "Hot"}]))
  (t/testing "retrieve data"
    (t/is (= [{:name "Ivan" :planet "earth"}]
             (query "SELECT PERSON.NAME,PLANET.NAME as PLANET FROM PERSON INNER JOIN PLANET ON PLANET = PLANET.NAME")))))

(t/deftest test-table-backed-by-query
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/columns [{:crux.db/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.db/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.db/attribute :planet
                                                 :crux.sql.column/name "planet"
                                                 :crux.sql.column/type :varchar}]
                       :crux.sql.table/query '[[?e :planet "earth"]]}])
  (f/transact! *api* (f/people [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                                {:crux.db/id :person/igor :name "Igor" :planet "not-earth"}]))
  (t/testing "retrieve data"
    (t/is (= #{{:id ":person/ivan", :name "Ivan", :planet "earth"}}
             (set (query "SELECT * FROM PERSON"))))))
