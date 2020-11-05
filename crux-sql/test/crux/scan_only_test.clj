(ns crux.scan-only-test
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.calcite :as cf :refer [explain query]]))

(defn- with-each-connection-type [f]
  (cf/with-calcite-connection-scan-only f))

(defn- with-sql-schema [f]
  (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query '{:find [?id ?name ?homeworld ?age ?alive]
                                                 :where [[?id :name ?name]
                                                         [?id :homeworld ?homeworld]
                                                         [?id :age ?age]
                                                         [?id :alive ?alive]]}
                         :crux.sql.table/columns '{?id :keyword, ?name :varchar, ?homeworld :varchar, ?age :bigint ?alive :boolean}}])
  (f))

(t/use-fixtures :each cf/with-calcite-module cf/with-scan-only fix/with-node with-each-connection-type with-sql-schema)

(t/deftest test-sql-query
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

  (let [q "SELECT CEIL(21) FROM PERSON"]
    (t/is (= 21
             (val (ffirst (query q)))))
    (t/is (= (str "EnumerableCalc(expr#0..4=[{inputs}], expr#5=[21], expr#6=[CEIL($t5)], EXPR$0=[$t6])\n"
                  "  CruxToEnumerableConverter\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (t/testing "retrieve data"
    (let [q "SELECT PERSON.NAME FROM PERSON"]
      (t/is (= #{{:name "Ivan"}
                 {:name "Malcolm"}}
               (set (query q))))
      (t/is (= (str "EnumerableCalc(expr#0..4=[{inputs}], NAME=[$t1])\n"
                    "  CruxToEnumerableConverter\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "retrieve data case insensitivity of table schema"
      (t/is (= #{{:name "Ivan"}
                 {:name "Malcolm"}}
               (set (query "select person.name from person"))))))

  (t/testing "order by"
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC")))
    (t/is (= [{:name "Malcolm"}
              {:name "Ivan"}]
             (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME DESC"))))

  (t/testing "multiple columns"
    (t/is (= #{{:name "Ivan" :homeworld "Earth"}
               {:name "Malcolm" :homeworld "Mars"}}
             (set (query "SELECT PERSON.NAME,PERSON.HOMEWORLD FROM PERSON")))))

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
               (:plan (first (query (str "explain plan for " q))))))))

  (t/testing "in operand"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME in ('Ivan')")))))

  (t/testing "and"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan' AND HOMEWORLD = 'Earth'")))))

  (t/testing "or"
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan' OR NAME = 'Malcolm'"))))
    (t/is (= #{{:name "Ivan"} {:name "Malcolm"}}
             (set (query "SELECT NAME FROM PERSON WHERE NAME = 'Ivan' OR AGE = 25")))))

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
