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

(t/deftest test-project
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

  (let [q "SELECT PERSON.NAME FROM PERSON"]
    (t/is (= [{:name "Ivan"}
              {:name "Malcolm"}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.NAME, PERSON.HOMEWORLD FROM PERSON"]
    (t/is (= [{:name "Ivan" :homeworld "Earth"}
              {:name "Malcolm" :homeworld "Mars"}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1], HOMEWORLD=[$2])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.HOMEWORLD, PERSON.NAME FROM PERSON"]
    (t/is (= [{:name "Ivan" :homeworld "Earth"}
              {:name "Malcolm" :homeworld "Mars"}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(HOMEWORLD=[$2], NAME=[$1])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.NAME, PERSON.AGE FROM PERSON"]
    (t/is (= [{:name "Ivan" :age 21}
              {:name "Malcolm" :age 25}]
             (query q)))))

(t/deftest test-sql-query
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                                {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}]))

  (t/testing "retrieve data"
    (let [q "SELECT PERSON.NAME FROM PERSON"]
      (t/is (= [{:name "Ivan"}
                {:name "Malcolm"}]
               (query q)))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1])\n"
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
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1])\n"
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

(t/deftest test-cardinality
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld ["Mars" "Earth"] :age 25 :alive false}])

  (let [q "SELECT * FROM PERSON WHERE HOMEWORLD = 'Earth'"]
    (t/is (= ["Ivan" "Malcolm"] (sort (map :name (query q))))))

  (let [q "SELECT * FROM PERSON"]
    (t/is (= ["Ivan" "Malcolm" "Malcolm"] (sort (map :name (query q)))))))

(t/deftest test-limit-and-offset
  (f/transact! *api* (for [i (range 20)]
                       {:crux.db/id (keyword (str "ivan" i)) :name "Ivan" :homeworld nil :age 21 :alive true}))

  (let [q "SELECT * FROM PERSON WHERE NAME='Ivan'"]
    (t/is (= 20 (count (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxFilter(condition=[=($1, 'Ivan')])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT * FROM PERSON WHERE NAME='Ivan' LIMIT 10"]
    (t/is (= 10 (count (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxSort(fetch=[10])\n"
                  "    CruxFilter(condition=[=($1, 'Ivan')])\n"
                  "      CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT * FROM PERSON WHERE NAME='Ivan' LIMIT 10 OFFSET 15"]
    (t/is (= 5 (count (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxSort(offset=[15], fetch=[10])\n"
                  "    CruxFilter(condition=[=($1, 'Ivan')])\n"
                  "      CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q)))))

(t/deftest test-sort
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}
                      {:crux.db/id :fred :name "Fred" :homeworld "Mars" :age 90 :alive false}])

  (let [q "SELECT NAME FROM PERSON ORDER BY NAME"]
    (t/is (= ["Fred" "Ivan" "Malcolm"] (map :name (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxSort(sort0=[$0], dir0=[ASC])\n"
                  "    CruxProject(NAME=[$1])\n"
                  "      CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT NAME FROM PERSON ORDER BY NAME DESC"]
    (t/is (= ["Malcolm" "Ivan" "Fred"] (map :name (query q)))))

  (let [q "SELECT NAME FROM PERSON ORDER BY HOMEWORLD DESC, AGE"]
    (t/is (= ["Malcolm" "Fred" "Ivan"] (map :name (query q)))))

  (let [q "SELECT NAME FROM PERSON ORDER BY HOMEWORLD DESC, AGE DESC"]
    (t/is (= ["Fred" "Malcolm" "Ivan"] (map :name (query q))))))

(t/deftest test-different-data-types
  (let [born #inst "2010"
        afloat (float 1.0)]
    (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query '{:find [?id ?name ?born ?afloat]
                                                 :where [[?id :name ?name]
                                                         [?id :born ?born]
                                                         [?id :afloat ?afloat]]}
                         :crux.sql.table/columns '{?id :keyword
                                                   ?name :varchar
                                                   ?born :timestamp
                                                   ?afloat :float}}
                        {:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :born born :afloat afloat}])
    (t/is (= [{:id ":human/ivan", :name "Ivan" :born born :afloat afloat}] (query "SELECT * FROM PERSON"))))
  (t/testing "restricted types"
    (t/is (thrown-with-msg? java.lang.IllegalArgumentException #"Unrecognised java.sql.Types: :time"
                            (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                                                 :crux.sql.table/name "person"
                                                 :crux.sql.table/query '{:find [?id ?name ?born]}
                                                 :crux.sql.table/columns '{?id :keyword
                                                                           ?born :time}}])))))

(t/deftest test-simple-joins
  (f/transact! *api* '[{:crux.db/id :crux.sql.schema/person
                        :crux.sql.table/name "person"
                        :crux.sql.table/query {:find [id name planet age]
                                               :where [[id :name name]
                                                       [id :planet planet]
                                                       [id :age age]]}
                        :crux.sql.table/columns {id :keyword, name :varchar planet :varchar age :bigint}}
                       {:crux.db/id :crux.sql.schema/planet
                        :crux.sql.table/name "planet"
                        :crux.sql.table/query {:find [id name climate]
                                               :where [[id :name name]
                                                       [id :climate climate]]}
                        :crux.sql.table/columns {id :keyword, name :varchar climate :varchar}}
                       {:crux.db/id :crux.sql.schema/ship
                        :crux.sql.table/name "ship"
                        :crux.sql.table/query {:find [id name captain decks]
                                               :where [[id :name name]
                                                       [id :captain captain]
                                                       [id :decks decks]]}
                        :crux.sql.table/columns {id :keyword, name :varchar captain :varchar decks :bigint}}])
  (f/transact! *api* [{:crux.db/id :person/ivan :name "Ivan" :planet "earth" :age 21}
                      {:crux.db/id :person/malcolm :name "Malcolm" :planet "mars" :age 21}
                      {:crux.db/id :planet/earth :name "earth" :climate "Hot"}
                      {:crux.db/id :ship/enterprise :name "enterprise" :captain "Ivan" :decks 13}])

  (let [q "SELECT * FROM PERSON INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME"]
    (t/is (= [{:id ":person/ivan",
               :name "Ivan",
               :planet "earth",
               :age 21,
               :id0 ":planet/earth",
               :name0 "earth",
               :climate "Hot"}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxJoin(condition=[=($2, $5)], joinType=[inner])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n"
                  "    CruxTableScan(table=[[crux, PLANET]])\n")
             (explain q))))

  (let [q "SELECT PERSON.ID, PERSON.NAME AS PERSON, PLANET.NAME AS PLANET FROM PERSON INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME"]
    (t/is (= [{:id ":person/ivan", :person "Ivan", :planet "earth"}]
             (query q)))

    (t/is (= (str "EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], PLANET=[$t3])\n"
                  "  CruxToEnumerableConverter\n"
                  "    CruxJoin(condition=[=($2, $3)], joinType=[inner])\n"
                  "      CruxProject(ID=[$0], NAME=[$1], PLANET=[$2])\n"
                  "        CruxTableScan(table=[[crux, PERSON]])\n"
                  "      CruxProject(NAME=[$1])\n"
                  "        CruxTableScan(table=[[crux, PLANET]])\n")
             (explain q))))

  (let [q (str "SELECT * FROM PERSON\n"
                 " INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME\n"
                 " INNER JOIN SHIP ON SHIP.CAPTAIN = PERSON.NAME")]
      (t/is (= [{:id ":person/ivan",
                 :name "Ivan",
                 :planet "earth",
                 :age 21,
                 :id0 ":planet/earth",
                 :name0 "earth",
                 :climate "Hot",
                 :id1 ":ship/enterprise",
                 :name1 "enterprise",
                 :captain "Ivan",
                 :decks 13}]
               (query q)))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxJoin(condition=[=($9, $1)], joinType=[inner])\n"
                    "    CruxJoin(condition=[=($2, $5)], joinType=[inner])\n"
                    "      CruxTableScan(table=[[crux, PERSON]])\n"
                    "      CruxTableScan(table=[[crux, PLANET]])\n"
                    "    CruxTableScan(table=[[crux, SHIP]])\n")
               (explain q)))))

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
