(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf :refer [query]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

;; See https://github.com/juxt/crux/issues/514

(defn- with-each-connection-type [f]
  (cf/with-calcite-connection f)
  (t/testing "With Avatica Connection"
    (cf/with-avatica-connection f)))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node with-each-connection-type)

(t/deftest test-sql-query
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/columns [{:crux.sql.column/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.sql.column/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.sql.column/attribute :homeworld
                                                 :crux.sql.column/name "homeworld"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.sql.column/attribute :alive
                                                 :crux.sql.column/name "alive"
                                                 :crux.sql.column/type :boolean}]}])
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :alive true}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :alive false}]))

  (t/testing "order by"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC")))
    (t/is (= [{:name "Malcolm"}
              {:name "Ivan"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME DESC"))))

  (t/testing "retrieve data"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON"))))

  (t/testing "multiple columns"
    (t/is (= [{:name "Ivan" :homeworld "Earth"}
              {:name "Malcolm" :homeworld "Mars"}]
             (query "SELECT PERSON.NAME,PERSON.HOMEWORLD FROM PERSON"))))

  (t/testing "wildcard columns"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan" :alive true}
               {:name "Malcolm" :homeworld "Mars" :id ":malcolm" :alive false}}
             (set (query "SELECT * FROM PERSON")))))

  (t/testing "equals operand"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan'"))))
    (t/is (= #{{:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME <> 'Ivan'"))))
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE 'Ivan' = NAME")))))

  (t/testing "in operand"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME in ('Ivan')")))))

  (t/testing "and"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan' AND HOMEWORLD = 'Earth'")))))

  (t/testing "or"
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan' OR NAME = 'Malcolm'")))))

  (t/testing "boolean"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE ALIVE = TRUE"))))
    (t/is (= #{{:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE ALIVE = FALSE")))))

  (t/testing "like"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME LIKE 'Iva%'"))))
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME LIKE 'Iva%' OR NAME LIKE 'Mal%'")))))

  (t/testing "arbitrary sql function"
    (t/is (= #{{:name "Iva"}}
             (set (query "SELECT SUBSTRING(NAME,1,3) AS NAME FROM PERSON WHERE NAME = 'Ivan'")))))

  (t/testing "namespaced keywords"
    (f/transact! *api* (f/people [{:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :alive true}]))
    (t/is (= [{:id ":human/ivan", :name "Ivan"}] (query "SELECT ID,NAME FROM PERSON WHERE ID = CRUXID('human/ivan')"))))

  (t/testing "numeric values"
    (fapi/delete-all-entities)
    (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/columns [{:crux.sql.column/attribute :name
                                                   :crux.sql.column/name "name"
                                                   :crux.sql.column/type :varchar}
                                                  {:crux.sql.column/attribute :age
                                                   :crux.sql.column/name "age"
                                                   :crux.sql.column/type :long}]}])
    (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :age 21}
                                  {:crux.db/id :malcolm :name "Malcolm" :age 25}]))

    (t/is (= [{:name "Ivan" :age 21}]
             (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE = 21")))

    (t/testing "Range"
      (t/is (= ["Malcolm"]
               (map :name (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE > 21"))))
      (t/is (= ["Ivan"]
               (map :name (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE 23 > AGE"))))
      (t/is (= #{"Ivan" "Malcolm"}
               (set (map :name (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE >= 21")))))
      (t/is (= ["Ivan"]
               (map :name (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE < 22"))))
      (t/is (= ["Ivan"]
               (map :name (query "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE <= 21")))))

    (t/testing "order by"
      (t/is (= [{:name "Ivan"}
                {:name "Malcolm"}]
               (query "SELECT PERSON.NAME FROM PERSON ORDER BY AGE ASC")))
      (t/is (= [{:name "Malcolm"}
                {:name "Ivan"}]
               (query "SELECT PERSON.NAME FROM PERSON ORDER BY AGE DESC")))))

  (t/testing "equality of columns"
    (fapi/delete-all-entities)
    (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/columns [{:crux.sql.column/attribute :name
                                                   :crux.sql.column/name "name"
                                                   :crux.sql.column/type :varchar}
                                                  {:crux.sql.column/attribute :surname
                                                   :crux.sql.column/name "surname"
                                                   :crux.sql.column/type :varchar}]}])
    (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :surname "Ivan"}
                                  {:crux.db/id :malcolm :name "Malcolm" :surname "Sparks"}]))
    (t/is (= [{:name "Ivan"}]
             (query "SELECT PERSON.NAME FROM PERSON WHERE NAME = SURNAME"))))

  (t/testing "unknown column"
    (t/is (thrown-with-msg? java.sql.SQLException #"Column 'NOCNOLUMN' not found in any table"
                            (query "SELECT NOCNOLUMN FROM PERSON")))))

(t/deftest test-simple-joins
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/columns [{:crux.sql.column/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.sql.column/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.sql.column/attribute :planet
                                                 :crux.sql.column/name "planet"
                                                 :crux.sql.column/type :varchar}]}
                      {:crux.db/id :crux.sql.schema/planet
                       :crux.sql.table/name "planet"
                       :crux.sql.table/columns [{:crux.sql.column/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.sql.column/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.sql.column/attribute :climate
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
                       :crux.sql.table/columns [{:crux.sql.column/attribute :crux.db/id
                                                 :crux.sql.column/name "id"
                                                 :crux.sql.column/type :keyword}
                                                {:crux.sql.column/attribute :name
                                                 :crux.sql.column/name "name"
                                                 :crux.sql.column/type :varchar}
                                                {:crux.sql.column/attribute :planet
                                                 :crux.sql.column/name "planet"
                                                 :crux.sql.column/type :varchar}]
                       :crux.sql.table/query '[[?e :planet "earth"]]}])
  (f/transact! *api* (f/people [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                                {:crux.db/id :person/igor :name "Igor" :planet "not-earth"}]))
  (t/testing "retrieve data"
    (t/is (= #{{:id ":person/ivan", :name "Ivan", :planet "earth"}}
             (set (query "SELECT * FROM PERSON"))))))
