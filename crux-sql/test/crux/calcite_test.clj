(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.calcite :as cf :refer [explain prepared-query query]])
  (:import [java.time ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter))

(defn- with-each-connection-type [f]
  (cf/with-calcite-connection f)
  (t/testing "With Avatica Connection"
    (cf/with-avatica-connection f)))

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

(t/use-fixtures :each cf/with-calcite-module fix/with-node with-each-connection-type with-sql-schema)

(defn- inst->iso-str [^java.util.Date t]
  (.format (ZonedDateTime/ofInstant (.toInstant t) (ZoneId/of "UTC")) DateTimeFormatter/ISO_INSTANT))

(t/deftest test-valid-time
  (let [id (java.util.UUID/randomUUID)
        tx1 (submit+await-tx [[:crux.tx/put {:crux.db/id id :name "Ivan" :homeworld (str id) :age 21 :alive true} #inst "2015"]
                              [:crux.tx/put {:crux.db/id id :name "Ivana" :homeworld (str id) :age 21 :alive true} #inst "2018"]])
        q (str "SELECT PERSON.NAME FROM PERSON WHERE HOMEWORLD = '" (str id) "'")]

    (t/is (= "Ivana" (:name (first (query q)))))
    (t/is (= "Ivan" (:name (first (query (str "VALIDTIME ('2016-12-01T10:13:30Z') " q))))))
    (t/testing "new lines and spacing"
      (t/is (= "Ivan" (:name (first (query (str "VALIDTIME('2016-12-01T10:13:30Z') \n " q)))))))

    (t/testing "RFC 3339"
      (t/is (= "Ivan" (:name (first (query (str "VALIDTIME('2016-12-01') \n " q))))))
      (t/is (= "Ivan" (:name (first (query (str "VALIDTIME('2016-12') \n " q))))))
      (t/is (= "Ivan" (:name (first (query (str "VALIDTIME('2016') \n " q))))))
      (t/is (= "Ivan" (:name (first (query (str "VALIDTIME('2016-12-01T10:13') \n " q)))))))

    (t/testing "Invalid String"
      (t/is (thrown-with-msg? java.lang.Exception #"Unrecognized date/time syntax: 2016-12-01TWOT"
                              (query (str "VALIDTIME('2016-12-01TWOT') \n " q)))))

    (submit+await-tx [[:crux.tx/put {:crux.db/id id :name "Ivanb" :homeworld (str id) :age 21 :alive true} #inst "2016"]])
    (assert (= "Ivana" (:name (first (query q)))))
    (assert (= "Ivanb" (:name (first (query (str "VALIDTIME ('2016-12-01T10:13:30Z') " q))))))

    (t/testing "tx-time"
      (t/is (= "Ivan" (:name (first (query (str (format "VALIDTIME ('2016-12-01T10:13:30Z') TRANSACTIONTIME ('%s') " (inst->iso-str (:crux.tx/tx-time tx1))) q)))))))))

(t/deftest test-project
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

  (let [q "SELECT PERSON.NAME FROM PERSON"]
    (t/is (= #{{:name "Ivan"}
               {:name "Malcolm"}}
             (set (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.NAME, PERSON.HOMEWORLD FROM PERSON"]
    (t/is (= #{{:name "Ivan" :homeworld "Earth"}
               {:name "Malcolm" :homeworld "Mars"}}
             (set (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1], HOMEWORLD=[$2])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.HOMEWORLD, PERSON.NAME FROM PERSON"]
    (t/is (= #{{:name "Ivan" :homeworld "Earth"}
               {:name "Malcolm" :homeworld "Mars"}}
             (set (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(HOMEWORLD=[$2], NAME=[$1])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q))))

  (let [q "SELECT PERSON.NAME, PERSON.AGE FROM PERSON"]
    (t/is (= [{:name "Ivan" :age 21}
              {:name "Malcolm" :age 25}]
             (sort-by :name (query q)))))

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
    (t/is (= #{{:name "Ivan", :double_age 42} {:name "Malcolm", :double_age 50}}
             (set (query q))))
    (t/is (= (str "CruxToEnumerableConverter\n"
                  "  CruxProject(NAME=[$1], DOUBLE_AGE=[*(2, $3)])\n"
                  "    CruxTableScan(table=[[crux, PERSON]])\n")
             (explain q)))))

(t/deftest test-project-literals-tpch-016
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

  (t/testing "tpch-016 project literals"
    (t/is (= #{{:name "Ivan" :t true}}
             (set (query "SELECT NAME, TRUE AS T FROM PERSON WHERE ALIVE = TRUE"))))
    (t/is (= #{{:name "Ivan" :t false}}
             (set (query "SELECT NAME, FALSE AS T FROM PERSON WHERE ALIVE = TRUE"))))
    (t/is (= #{{:name "Ivan" :t 1}}
             (set (query "SELECT NAME, 1 AS T FROM PERSON WHERE ALIVE = TRUE"))))
    (t/is (= #{{:name "Ivan" :t "h"}}
             (set (query "SELECT NAME, 'h' AS T FROM PERSON WHERE ALIVE = TRUE")))))

  (t/is (= #{{:name "Ivan", :t 1} {:name "Malcolm", :t 1}}
           (set (query "SELECT NAME, 1 AS T FROM PERSON")))))

(t/deftest test-sql-query
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])

  (t/testing "count"
    (let [q "SELECT count(*) as N FROM PERSON"]
      (t/is (= [{:n 2}]
               (query q)))
      (t/is (= (str "EnumerableAggregate(group=[{}], N=[COUNT()])\n"
                    "  CruxToEnumerableConverter\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "retrieve data case insensitivity of table schema"
      (t/is (= #{{:name "Ivan"}
                 {:name "Malcolm"}}
               (set (query "select person.name from person"))))))

  (t/testing "retrieve data"
    (let [q "SELECT PERSON.NAME FROM PERSON"]
      (t/is (= #{{:name "Ivan"}
                 {:name "Malcolm"}}
               (set (query q))))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1])\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "retrieve data case insensitivity of table schema"
      (t/is (= #{{:name "Ivan"}
                 {:name "Malcolm"}}
               (set (query "select person.name from person"))))))

  (t/testing "order by"
    (t/is (= #{{:name "Ivan"}
               {:name "Malcolm"}}
             (set (query "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC"))))
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

(t/deftest test-booleans
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= #{{:name "Ivan"}}
           (set (query "SELECT NAME FROM PERSON WHERE ALIVE = TRUE"))))
  (t/is (= #{{:name "Malcolm"}}
           (set (query "SELECT NAME FROM PERSON WHERE ALIVE = FALSE"))))
  (t/is (= #{{:name "Ivan"}}
           (set (query "SELECT NAME FROM PERSON WHERE NAME IS NOT NULL OR ALIVE = TRUE")))))

(t/deftest test-calcs
  (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query '{:find [?id ?name ?age ?years_worked]
                                                 :where [[?id :name ?name]
                                                         [?id :age ?age]
                                                         [?id :years_worked ?years_worked]]}
                         :crux.sql.table/columns '{?id :keyword, ?name :varchar, ?age :bigint, ?years_worked :bigint}}])

  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :age 42 :years_worked 21}
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
               (sort-by :name (query q))))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1], AGE=[*($2, 2)])\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q))))

    (t/testing "nested"
      (let [q "SELECT NAME, ((PERSON.AGE * 2) * 3) AS AGE FROM PERSON"]
        (t/is (= [{:name "Ivan", :age 252} {:name "Malcolm", :age 132}]
                 (sort-by :name (query q)))))))

  (t/testing "in OR conditional with args"
    (let [q "SELECT NAME FROM PERSON WHERE NAME = 'Malcolm' OR AGE = (2 * YEARS_WORKED)"]
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(NAME=[$1])\n"
                    "    CruxFilter(condition=[OR(=($1, 'Malcolm'), =($2, *(2, $3)))])\n"
                    "      CruxTableScan(table=[[crux, PERSON]])\n")
               (explain q)))
      (t/is (= [{:name "Ivan"} {:name "Malcolm"}]
               (sort-by :name (query q))))))

  (t/testing "tphc-022-example-substring"
    (let [q "SELECT NAME FROM PERSON WHERE substring(name from 1 for 1) in ('I', 'V')"]
      (t/is (= [{:name "Ivan"}]
               (query q))))))

(t/deftest test-keywords
  (fix/transact! *api* [{:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :alive true :age 21}])

  (t/testing "select"
    (t/is (= [{:id ":human/ivan"}] (query "SELECT ID FROM PERSON"))))

  (t/testing "filter"
    (t/is (= [{:id ":human/ivan", :name "Ivan"}] (query "SELECT ID,NAME FROM PERSON WHERE ID = KEYWORD('human/ivan')")))))

(t/deftest test-uuid
  (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query '{:find [?id ?name ?auuid]
                                                 :where [[?id :name ?name]
                                                         [?id :auuid ?auuid]]}
                         :crux.sql.table/columns '{?id :keyword, ?name :varchar, ?auuid :uuid}}])
  (let [auuid (java.util.UUID/randomUUID)]
    (fix/transact! *api* [{:crux.db/id :human/ivan :name "Ivan" :auuid auuid}])
    (t/is (= [{:id ":human/ivan",
               :name "Ivan",
               :auuid (str auuid)}]
             (query "SELECT * FROM PERSON")))

    (t/testing "filter"
      (t/is (= [{:name "Ivan"}] (query (format "SELECT NAME FROM PERSON WHERE AUUID = UUID('%s')" auuid)))))))

(t/deftest test-equality-of-columns
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Ivan" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= [{:name "Ivan"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE NAME = HOMEWORLD"))))

(t/deftest test-query-for-null
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld nil :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= [{:name "Ivan"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE HOMEWORLD IS NULL")))
  (t/is (= [{:name "Malcolm"}]
           (query "SELECT PERSON.NAME FROM PERSON WHERE HOMEWORLD IS NOT NULL")))
  (t/is (= 2 (count (query "SELECT PERSON.NAME FROM PERSON WHERE 'FOO' IS NOT NULL")))))

(t/deftest test-cardinality
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name "Malcolm" :homeworld ["Mars" "Earth"] :age 25 :alive false}])

  (t/is (= #{["Ivan"] ["Malcolm"]} (c/q (c/db *api*)
                                        '{:find [?name],
                                          :where [[?id :name ?name]
                                                  [?id :homeworld ?homeworld]
                                                  [?id :alive ?alive]
                                                  [(= ?homeworld "Earth")]]})))

  (t/is (= #{["Ivan"] ["Malcolm"]} (c/q (c/db *api*)
                                        '{:find [?name],
                                          :where [[?id :name ?name]
                                                  [?id :homeworld ?homeworld]
                                                  [?id :alive ?alive]
                                                  [(= ?homeworld G__158554)]],
                                          :args [{G__158554 "Earth"}]})))

  (let [q "SELECT * FROM PERSON WHERE HOMEWORLD = 'Earth'"]
    (t/is (= ["Ivan" "Malcolm"] (sort (map :name (query q))))))

  (let [q "SELECT * FROM PERSON"]
    (t/is (= ["Ivan" "Malcolm" "Malcolm"] (sort (map :name (query q)))))))

(t/deftest test-limit-and-offset
  (fix/transact! *api* (for [i (range 20)]
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
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
                        {:crux.db/id :malcolm :name" Malcolm" :homeworld "Mars" :age 25 :alive false}])
  (t/is (= [{:homeworld "Earth"}] (prepared-query "SELECT HOMEWORLD FROM PERSON WHERE NAME = ?" [1 "Ivan"])))
  (t/is (= [{:homeworld "Earth"}] (prepared-query "SELECT HOMEWORLD FROM PERSON WHERE TRIM(NAME) = ?" [1 "Ivan"])))
  (t/is (= [{:name " Malcolm"}] (prepared-query "SELECT NAME FROM PERSON WHERE AGE > ?" [1 23]))))

(t/deftest test-sort
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :age 21 :alive true}
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
  (let [born #inst "2010-07-01"
        afloat (float 1.0)]
    (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                           :crux.sql.table/name "person"
                           :crux.sql.table/query '{:find [?id ?name ?born ?afloat ?adecimal]
                                                   :where [[?id :name ?name]
                                                           [?id :born ?born]
                                                           [?id :afloat ?afloat]
                                                           [?id :adecimal ?adecimal]]}
                           :crux.sql.table/columns '{?id :keyword
                                                     ?name :varchar
                                                     ?born :timestamp
                                                     ?afloat :float
                                                     ?adecimal :decimal}}
                          {:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :born born :afloat afloat :adecimal 1.3M}])
    (t/is (= [{:id ":human/ivan", :name "Ivan" :born born :afloat afloat :adecimal 1.3M}] (query "SELECT * FROM PERSON")))
    (t/is (first (query "SELECT NAME FROM PERSON WHERE ADECIMAL = 1.3"))))
  (t/testing "restricted types"
    (t/is (thrown-with-msg? java.lang.Exception #"Unrecognised java.sql.Types: :time"
                            (do
                              (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                                                     :crux.sql.table/name "person"
                                                     :crux.sql.table/query '{:find [?id ?name ?born]}
                                                     :crux.sql.table/columns '{?id :keyword
                                                                               ?born :time}}])
                              (query "SELECT * FROM PERSON")))))
  (t/testing "missing column definition"
    (t/is (thrown-with-msg? java.lang.Exception #"Unrecognised column: \?name"
                            (do
                              (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                                                     :crux.sql.table/name "person"
                                                     :crux.sql.table/query '{:find [?id ?name]}
                                                     :crux.sql.table/columns '{?id :keyword}}])
                              (query "SELECT * FROM PERSON"))))))

(t/deftest test-simple-joins
  (fix/transact! *api* '[{:crux.db/id :crux.sql.schema/person
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
  (fix/transact! *api* [{:crux.db/id :person/ivan :name "Ivan" :planet "earth" :age 25}
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
             (sort-by :name (query q))))
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
  (fix/transact! *api* [{:crux.db/id :crux.sql.schema/person
                         :crux.sql.table/name "person"
                         :crux.sql.table/query {:find ['id 'name 'planet]
                                                :where [['id :name 'name]
                                                        ['id :planet 'planet]
                                                        ['id :planet "earth"]]}
                         :crux.sql.table/columns {'id :keyword, 'name :varchar 'planet :varchar}}])
  (fix/transact! *api* [{:crux.db/id :person/ivan :name "Ivan" :planet "earth"}
                        {:crux.db/id :person/igor :name "Igor" :planet "not-earth"}])
  (t/testing "retrieve data"
    (t/is (= #{{:id ":person/ivan", :name "Ivan", :planet "earth"}}
             (set (query "SELECT * FROM PERSON"))))))

(t/deftest test-arithmetic
  (fix/transact! *api* [{:crux.db/id :human/ivan :name "Ivan" :homeworld "Earth" :alive true :age 21}])

  (t/is (= [{:age 10}] (query "SELECT (AGE / 2) AS AGE FROM PERSON")))
  (t/is (= [{:age 42}] (query "SELECT (AGE * 2) AS AGE FROM PERSON")))
  (t/is (= [{:age 23}] (query "SELECT (AGE + 2) AS AGE FROM PERSON")))
  (t/is (= [{:age 19}] (query "SELECT (AGE - 2) AS AGE FROM PERSON")))
  (t/is (= [{:age 3}] (query "SELECT mod(AGE, 6) AS AGE FROM PERSON")))
  (t/is (= [{:age 5}] (query "SELECT mod((AGE + 2), 6) AS AGE FROM PERSON"))))

(t/deftest test-calcite-built-in-fns
  (fix/transact! *api* [{:crux.db/id :human/ivan :name " Ivan " :homeworld "earth" :alive true :age 21}])

  (t/testing "single arg fns"
    (t/is (= [{:lname " ivan "}] (query "SELECT LOWER(NAME) AS LNAME FROM PERSON")))
    (t/is (= [{:lname " IVAN "}] (query "SELECT UPPER(NAME) AS LNAME FROM PERSON")))
    (t/is (= [{:planet "Earth"}] (query "SELECT INITCAP(HOMEWORLD) AS PLANET FROM PERSON"))))

  (t/testing "literals"
    (t/is (= [{:lname " ivan "}] (query "SELECT LOWER(' IVAN ') AS LNAME FROM PERSON"))))
  (t/testing "nested"
    (t/is (= [{:planet "Earth"}] (query "SELECT INITCAP(LOWER(HOMEWORLD)) AS PLANET FROM PERSON"))))

  (t/is (= "Ivan" (:name2 (first (query "SELECT TRIM(NAME) AS NAME2 FROM PERSON")))))
  (t/is (= " Ivan qs" (:name2 (first (query "SELECT NAME, {fn CONCAT(NAME, 'qs')} AS NAME2 FROM PERSON")))))
  (t/is (= "Ivan qs" (:name2 (first (query "SELECT TRIM({fn CONCAT(NAME, 'qs')}) AS NAME2 FROM PERSON")))))

  (t/testing "ceil"
    (t/is (= 21 (:age (first (query "SELECT CEIL(AGE) AS AGE FROM PERSON")))))
    (t/is (= 1 (:age (first (query "SELECT CEIL(1) AS AGE FROM PERSON")))))
    (t/is (first (query "SELECT NAME FROM PERSON WHERE CEIL(AGE) = 21")))

    (let [q "SELECT CEIL(1.1) FROM PERSON"]
      (t/is (= 2M (val (ffirst (query q)))))
      (t/is (= (str "CruxToEnumerableConverter\n"
                    "  CruxProject(EXPR$0=[CEIL(1.1:DECIMAL(2, 1))])\n"
                    "    CruxTableScan(table=[[crux, PERSON]])\n") (explain q))))
    (t/is (= 1M (val (ffirst (query "SELECT FLOOR(1.1) FROM PERSON"))))))

  (let [q  "SELECT TRUNCATE(1.12, 1) FROM PERSON"]
    (t/is (= 1.1M (val (ffirst (query q))))))

  ;; Todo, edge case not working:
  #_(let [q  "SELECT TRUNCATE(1.12, TRUNCATE(1)) FROM PERSON"]
    (t/is (= 1.1M (val (ffirst (query q))))))

  (let [q  "SELECT REPLACE(NAME, 'v', 'A') FROM PERSON"]
    (t/is (= " IAan " (val (ffirst (query q))))))

  (let [q  "SELECT CHAR_LENGTH(NAME), NAME FROM PERSON"]
    (t/is (= 6 (val (ffirst (query q))))))

  (t/is (:current_date (first (query "SELECT current_date FROM PERSON"))))
  (t/is (:current_time (first (query "SELECT current_time FROM PERSON"))))
  (t/is (:current_timestamp (first (query "SELECT current_timestamp FROM PERSON"))))
  (t/is (first (query "SELECT last_day(current_timestamp) FROM PERSON"))))

(comment
  (import '[ch.qos.logback.classic Level Logger]
          'org.slf4j.LoggerFactory)
  (.setLevel ^Logger (LoggerFactory/getLogger "crux.calcite") (Level/valueOf "DEBUG")))
