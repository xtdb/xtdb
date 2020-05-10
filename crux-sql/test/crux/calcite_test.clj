(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api* submit+await-tx]]
            [crux.fixtures.calcite :as cf :refer [explain prepared-query query]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.query :as q]))

(defn- with-each-connection-type [f]
  (cf/with-calcite-connection f)
  #_(t/testing "With Avatica Connection"
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

(t/deftest test-valid-time
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true} #inst "2015"]
                    [:crux.tx/put {:crux.db/id :ivan :name "Ivana" :homeworld "Earth" :age 21 :alive true}]
                    [:crux.tx/put {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}]])

  (let [q "VALIDTIME ('2016-12-01T10:13:30Z') SELECT PERSON.NAME FROM PERSON"]
    (t/is (= [{:name "Ivan"}]
             (query q))))

  (let [q "SELECT PERSON.NAME FROM PERSON"]
    (t/is (= [{:name "Ivana"}
              {:name "Malcolm"}]
             (query q)))))

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
             (query q))))

  (let [q "SELECT SUM(PERSON.AGE) AS TOTAL_AGE FROM PERSON"]
    (t/is (= [{:total_age 46}]
             (query q)))
    (t/is (= (str "EnumerableAggregate(group=[{}], TOTAL_AGE=[SUM($3)])\n"
                  "  CruxToEnumerableConverter\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT MAX(PERSON.AGE) AS MAX_AGE FROM PERSON"]
    (t/is (= [{:max_age 25}]
             (query q)))
    (t/is (= (str "EnumerableAggregate(group=[{}], MAX_AGE=[MAX($3)])\n"
                  "  CruxToEnumerableConverter\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.NAME, (2 * PERSON.AGE) AS DOUBLE_AGE FROM PERSON"]
    (t/is (= [{:name "Ivan", :double_age 42} {:name "Malcolm", :double_age 50}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1], DOUBLE_AGE=[*(2, $3)])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (t/testing "tpch-016 boolean literal in projection"
    (t/is (= #{{:name "Ivan"}}
             (set (query "SELECT NAME, TRUE FROM PERSON WHERE ALIVE = TRUE"))))))

(t/deftest test-sql-query
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

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

(t/deftest test-calcs
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/query '{:find [?id ?name ?age ?years_worked]
                                               :where [[?id :name ?name]
                                                       [?id :age ?age]
                                                       [?id :years_worked ?years_worked]]}
                       :crux.sql.table/columns '{?id :keyword, ?name :varchar, ?age :bigint, ?years_worked :bigint}}])

  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :age 42 :years_worked 21}
                      {:crux.db/id :malcolm :name "Malcolm" :age 22 :years_worked 10}])

  (t/testing "filter operators"
    (let [q "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE = (YEARS_WORKED * 2)"]
      (t/is (= ["Ivan"]
               (map :name (query q))))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1], AGE=[$2])\n"
                    "    CruxFilter(condition=[=($2, *($3, 2))])\n"
                    "      CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "nested"
      (let [q "SELECT PERSON.NAME,PERSON.AGE FROM PERSON WHERE AGE = (2 + (YEARS_WORKED * 2))"]
        (t/is (= ["Malcolm"]
                 (map :name (query q))))
        (t/is (= (str "CruxToEnumerableConverter\n"
                      "  CruxProject(NAME=[$1], AGE=[$2])\n"
                      "    CruxFilter(condition=[=($2, +(2, *($3, 2)))])\n"
                      "      CruxTableScan(table=[[crux, PERSON]])\n")
                 (explain q))))))

  (t/testing "project"
    (let [q "SELECT NAME, (PERSON.AGE * 2) AS AGE FROM PERSON"]
      (t/is (= [{:name "Ivan", :age 84} {:name "Malcolm", :age 44}]
               (query q)))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1], AGE=[*($2, 2)])\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "nested"
      (let [q "SELECT NAME, ((PERSON.AGE * 2) * 3) AS AGE FROM PERSON"]
        (t/is (= [{:name "Ivan", :age 252} {:name "Malcolm", :age 132}]
                 (query q))))))

  (t/testing "in OR conditional with args"
    (let [q "SELECT NAME FROM PERSON WHERE NAME = 'Malcolm' OR AGE = (2 * 21)"]
      (t/is (= [{:name "Ivan"} {:name "Malcolm"}]
               (query q)))))

  (t/testing "tphc-022-example-substring"
    (let [q "SELECT NAME FROM PERSON WHERE substring(name from 1 for 1) in ('I', 'V')"]
      (t/is (= [{:name "Ivan"}]
               (query q))))))

(t/deftest test-keywords
  (f/transact! *api* [{:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :alive true :age 21}])

  (t/testing "select keywords"
    (t/is (= [{:id ":human/ivan"}] (query "SELECT ID FROM PERSON"))))

  (t/testing "filter using keyword"
    (t/is (= [{:id ":human/ivan", :name "Ivan"}] (query "SELECT ID,NAME FROM PERSON WHERE ID = KEYWORD('human/ivan')")))))

(t/deftest test-equality-of-columns
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Ivan" :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= [{:name "Ivan"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE NAME = HOMEWORLD"))))

(t/deftest test-query-for-null
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld nil :age 21 :alive true}
                      {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])
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

(t/deftest test-prepare-statement
  (f/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                      {:crux.db/id :malcolm :name" Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= [{:homeworld "Earth"}] (prepared-query "SELECT HOMEWORLD FROM PERSON WHERE NAME = ?" [1 "Ivan"]))))

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
                        :crux.sql.table/query {:find [id name climate age]
                                               :where [[id :name name]
                                                       [id :climate climate]
                                                       [id :age age]]}
                        :crux.sql.table/columns {id :keyword, name :varchar climate :varchar age :bigint}}
                       {:crux.db/id :crux.sql.schema/ship
                        :crux.sql.table/name "ship"
                        :crux.sql.table/query {:find [id name captain decks]
                                               :where [[id :name name]
                                                       [id :captain captain]
                                                       [id :decks decks]]}
                        :crux.sql.table/columns {id :keyword, name :varchar captain :varchar decks :bigint}}])
  (f/transact! *api* [{:crux.db/id :person/ivan :name "Ivan" :planet "earth" :age 25}
                      {:crux.db/id :person/malcolm :name "Malcolm" :planet "mars" :age 21}
                      {:crux.db/id :planet/earth :name "earth" :climate "Hot" :age 42}
                      {:crux.db/id :ship/enterprise :name "enterprise" :captain "Ivan" :decks 13}])

  (let [q "SELECT * FROM PERSON INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME"]
    (t/is (= [{:id ":person/ivan",
               :name "Ivan",
               :planet "earth",
               :age 25,
               :id0 ":planet/earth",
               :name0 "earth",
               :climate "Hot"
               :age0 42}]
             (query q)))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxJoin(condition=[=($2, $5)], joinType=[inner])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n"
                  "    CruxTableScan(table=[[crux, PLANET]])\n")
             (explain q))))

  (t/testing "joins with projects"
    (let [q "SELECT PERSON.ID, PERSON.NAME AS PERSON, PLANET.NAME AS PLANET FROM PERSON INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME"]
      (t/is (= [{:id ":person/ivan", :person "Ivan", :planet "earth"}]
               (query q)))

      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(ID=[$1], PERSON=[$2], PLANET=[$0])\n"
                    "    CruxJoin(condition=[=($3, $0)], joinType=[inner])\n"
                    "      CruxProject(NAME=[$1])\n"
                    "        CruxTableScan(table=[[crux, PLANET]])\n"
                    "      CruxProject(ID=[$0], NAME=[$1], PLANET=[$2])\n"
                    "        CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "triple join"
      (let [q (str "SELECT * FROM PERSON\n"
                   " INNER JOIN PLANET ON PERSON.PLANET = PLANET.NAME\n"
                   " INNER JOIN SHIP ON SHIP.CAPTAIN = PERSON.NAME")]
        (t/is (= [{:id ":person/ivan",
                   :name "Ivan",
                   :planet "earth",
                   :age 25,
                   :id0 ":planet/earth",
                   :name0 "earth",
                   :climate "Hot",
                   :age0 42
                   :id1 ":ship/enterprise",
                   :name1 "enterprise",
                   :captain "Ivan",
                   :decks 13}]
                 (query q)))
        (t/is (= (str "CruxToEnumerableConverter\n"
                      "  CruxJoin(condition=[=($10, $1)], joinType=[inner])\n"
                      "    CruxJoin(condition=[=($2, $5)], joinType=[inner])\n"
                      "      CruxTableScan(table=[[crux, PERSON]])\n"
                      "      CruxTableScan(table=[[crux, PLANET]])\n"
                      "    CruxTableScan(table=[[crux, SHIP]])\n")
                 (explain q))))))

  (t/testing "join using calc"
    (let [q (str "SELECT * FROM PERSON INNER JOIN PLANET ON PLANET.AGE = (2 * PERSON.AGE)")]
      (t/is (= [{:id ":person/malcolm",
                 :name "Malcolm",
                 :planet "mars",
                 :age 21,
                 :id0 ":planet/earth",
                 :name0 "earth",
                 :climate "Hot",
                 :age0 42}]
               (query q)))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(ID=[$4], NAME=[$5], PLANET=[$6], AGE=[$7], ID0=[$0], NAME0=[$1], CLIMATE=[$2], AGE0=[$3])\n"
                    "    CruxJoin(condition=[=($3, $8)], joinType=[inner])\n"
                    "      CruxTableScan(table=[[crux, PLANET]])\n"
                    "      CruxProject(ID=[$0], NAME=[$1], PLANET=[$2], AGE=[$3], $f4=[*(2, $3)])\n"
                    "        CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q)))))

  (let [q "SELECT PERSON.NAME FROM PERSON LEFT OUTER JOIN PLANET ON PERSON.PLANET = PLANET.NAME"]
    ;; Calcite handles OUTER_JOINS for now:
    (t/is (= [{:name "Ivan"} {:name "Malcolm"}]
             (query q)))
    (t/is (= (str "EnumerableCalc(expr#0..2=[{inputs}], NAME=[$t0])\n"
                  "  EnumerableHashJoin(condition=[=($1, $2)], joinType=[left])\n"
                  "    CruxToEnumerableConverter\n"
                  "      CruxProject(NAME=[$1], PLANET=[$2])\n"
                  "        CruxTableScan(table=[[crux, PERSON]])\n"
                  "    CruxToEnumerableConverter\n"
                  "      CruxProject(NAME=[$1])\n"
                  "        CruxTableScan(table=[[crux, PLANET]])\n")
             (explain q)))))

(t/deftest test-table-backed-by-query
  (f/transact! *api* [{:crux.db/id :crux.sql.schema/person
                       :crux.sql.table/name "person"
                       :crux.sql.table/query {:find ['id 'name 'planet]
                                              :where [['id :name 'name]
                                                      ['id :planet 'planet]
                                                      ['id :planet "earth"]]}
                       :crux.sql.table/columns {'id :keyword, 'name :varchar 'planet :varchar}}])
  (f/transact! *api* [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                      {:crux.db/id :person/igor :name "Igor" :planet "not-earth"}])
  (t/testing "retrieve data"
    (t/is (= #{{:id ":person/ivan", :name "Ivan", :planet "earth"}}
             (set (query "SELECT * FROM PERSON"))))))


(comment
  (import '[ch.qos.logback.classic Level Logger]
          'org.slf4j.LoggerFactory)
  (.setLevel ^Logger (LoggerFactory/getLogger "crux.calcite") (Level/valueOf "DEBUG")))
