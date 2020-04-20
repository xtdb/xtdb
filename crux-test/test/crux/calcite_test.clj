(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf :refer [query explain]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

;; TODO align the test data/fixture code with people fixture

(defn- with-each-connection-type [f]
  (cf/with-calcite-connection f)
  (t/testing "With Avatica Connection"
    (cf/with-avatica-connection f)))

(defn- with-sql-schema [f]
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/query '{:find [?id ?name ?homeworld ?age ?alive]
                                               :where [[?id :name ?name]
                                                       [?id :homeworld ?homeworld]
                                                       [?id :age ?age]
                                                       [?id :alive ?alive]]}
                       :crux.sql.table/columns '{?id :keyword, ?name :varchar, ?homeworld :varchar, ?age :bigint ?alive :boolean}}])
  (f))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node with-each-connection-type with-sql-schema)

(t/deftest test-sql-query
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}]))

  (t/testing "retrieve data"
    (let [q "SELECT PERSON.NAME FROM PERSON"]
      (t/is (= [{:name "Ivan"}
                {:name "Malcolm"}]
               (query q)))
      (t/is (= (str "EnumerableCalc(expr#0..4=[{inputs}], NAME=[$t1])\n"
                    "  CruxToEnumerableConverter\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "retrieve data case insensitivity of table schema"
      (t/is (= [{:name "Ivan"}
                {:name "Malcolm"}]
               (query "select person.name from person")))))

  (t/testing "order by"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC")))
    (t/is (= [{:name "Malcolm"}
              {:name "Ivan"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME DESC"))))

  (t/testing "multiple columns"
    (t/is (= [{:name "Ivan" :homeworld "Earth"}
              {:name "Malcolm" :homeworld "Mars"}]
             (query "SELECT PERSON.NAME,PERSON.HOMEWORLD FROM PERSON"))))

  (t/testing "wildcard columns"
    (t/is (= #{{:name "Ivan" :homeworld "Earth" :id ":ivan" :age 21 :alive true}
               {:name "Malcolm" :homeworld "Mars" :id ":malcolm" :age 25 :alive false}}
             (set (query "SELECT * FROM PERSON")))))

  (t/testing "equals operand"
    (let [q "SELECT NAME FROM PERSON WHERE NAME = 'Ivan'"]
      (t/is (= #{{:name "Ivan"}}
               (set (query q))))
      (t/is (= (str "EnumerableCalc(expr#0..4=[{inputs}], NAME=[$t1])\n"
                    "  CruxToEnumerableConverter\n"
                    "    CruxFilter(condition=[=($1, 'Ivan')])\n"
                    "      CruxTableScan(table=[[crux, PERSON]])\n")
               (:plan (first (query (str "explain plan for " q)))))))
    (t/is (= #{{:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME <> 'Ivan'"))))
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE 'Ivan' = NAME"))))
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE 'Ivan' = 'Ivan'")))))

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

  (t/testing "numeric-columns"
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

  (t/testing "like"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME LIKE 'Iva%'"))))
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME LIKE 'Iva%' OR NAME LIKE 'Mal%'")))))

  (t/testing "arbitrary sql function"
    (t/is (= #{{:name "Iva"}}
             (set (query "SELECT SUBSTRING(NAME,1,3) AS NAME FROM PERSON WHERE NAME = 'Ivan'")))))

  (t/testing "unknown column"
    (t/is (thrown-with-msg? java.sql.SQLException #"Column 'NOCNOLUMN' not found in any table"
                            (query "SELECT NOCNOLUMN FROM PERSON")))))

(t/deftest test-namespaced-keywords
  (f/transact! *api* (f/people [{:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :alive true}]))
  (t/is (= [{:id ":human/ivan", :name "Ivan"}] (query "SELECT ID,NAME FROM PERSON WHERE ID = CRUXID('human/ivan')"))))

(t/deftest test-equality-of-columns
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Ivan" :age 21 :alive true}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}]))
  (t/is (= [{:name "Ivan"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE NAME = HOMEWORLD"))))

(t/deftest test-query-for-null
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld nil :age 21 :alive true}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}]))
  (t/is (= [{:name "Ivan"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE HOMEWORLD IS NULL")))
  (t/is (= [{:name "Malcolm"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE HOMEWORLD IS NOT NULL")))
  (t/is (= 2 (count (query "SELECT PERSON.NAME FROM PERSON WHERE 'FOO' IS NOT NULL")))))

(t/deftest test-different-data-types
  (let [born #inst "2010"]
    (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query '{:find [?id ?name ?born]
                                                 :where [[?id :name ?name]
                                                         [?id :born ?born]]}
                         :crux.sql.table/columns '{?id :keyword
                                                   ?name :varchar
                                                   ?born :timestamp}}
                        {:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :born born}])
    (t/is (= [{:id ":human/ivan", :name "Ivan" :born born}] (query "SELECT * FROM PERSON"))))
  (t/testing "restricted types"
    (t/is (thrown-with-msg? java.lang.IllegalArgumentException #"Unrecognised java.sql.Types: :time"
                            (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                                                 :crux.sql.table/name "person"
                                                 :crux.sql.table/query '{:find [?id ?name ?born]}
                                                 :crux.sql.table/columns '{?id :keyword
                                                                           ?born :time}}])))))

(t/deftest test-simple-joins
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/query {:find ['id 'name 'planet]
                                              :where [['id :name 'name]
                                                      ['id :planet 'planet]]}
                       :crux.sql.table/columns {'id :keyword, 'name :varchar 'planet :varchar}}
                      {:crux.db/id :crux.sql.schema/planet
                       :crux.sql.table/name "planet"
                       :crux.sql.table/query {:find ['id 'name 'climate]
                                              :where [['id :name 'name]
                                                      ['id :climate 'climate]]}
                       :crux.sql.table/columns {'id :keyword, 'name :varchar 'climate :varchar}}])
  (f/transact! *api* (f/people [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                                {:crux.db/id :planet/earth :name "earth" :climate "Hot"}]))
  (t/testing "retrieve data"
    (t/is (= [{:name "Ivan" :planet "earth"}]
             (query "SELECT PERSON.NAME,PLANET.NAME as PLANET FROM PERSON INNER JOIN PLANET ON PLANET = PLANET.NAME")))))

(t/deftest test-table-backed-by-query
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/query {:find ['id 'name 'planet]
                                              :where [['id :name 'name]
                                                      ['id :planet 'planet]
                                                      ['id :planet "earth"]]}
                       :crux.sql.table/columns {'id :keyword, 'name :varchar 'planet :varchar}}])
  (f/transact! *api* (f/people [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                                {:crux.db/id :person/igor :name "Igor" :planet "not-earth"}]))
  (t/testing "retrieve data"
    (t/is (= #{{:id ":person/ivan", :name "Ivan", :planet "earth"}}
             (set (query "SELECT * FROM PERSON"))))))
