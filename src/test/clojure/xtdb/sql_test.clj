(ns xtdb.sql-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types]
            [xtdb.util :as util]
            [xtdb.compactor :as c])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (xtdb.types RegClass RegProc)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(def regen-expected-files? false) ;; <<no-commit>>

(defmethod t/assert-expr '=plan-file [msg form]
  `(let [exp-plan-file-name# ~(nth form 1)
         exp-plan-file-path# (format "xtdb/sql/plan_test_expectations/%s.edn" exp-plan-file-name#)
         actual-plan# ~(nth form 2)]
     (binding [*print-namespace-maps* false]
       (when regen-expected-files?
         (when-not (io/resource exp-plan-file-path#)
           (Files/createFile (doto (util/->path (format "src/test/resources/%s" exp-plan-file-path#))
                               (-> .toFile io/make-parents))
                             (make-array FileAttribute 0)))
         (spit (io/resource exp-plan-file-path#) (with-out-str (clojure.pprint/pprint actual-plan#))))

       (if-let [exp-plan-file# (io/resource exp-plan-file-path#)]
         (let [exp-plan# (read-string (slurp exp-plan-file#))
               result# (= exp-plan# actual-plan#)]
           (if result#
             (t/do-report {:type :pass
                           :message ~msg
                           :expected (list '~'= exp-plan-file-name# actual-plan#)
                           :actual (list '~'= exp-plan# actual-plan#)})
             (t/do-report {:type :fail
                           :message ~msg
                           :expected (list '~'= exp-plan-file-name# actual-plan#)
                           :actual (list '~'not (list '~'= exp-plan# actual-plan#))}))
           result#)
         (spit (io/resource (str "xtdb/sql/plan_test_expectations/" exp-plan-file-name#))
               (with-out-str (clojure.pprint/pprint actual-plan#)))))))

(t/deftest test-basic-queries
  (t/is (=plan-file
         "basic-query-1"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si, movie_star AS ms WHERE si.star_name = ms.name AND ms.birthdate = 1960"
                   {:table-info {#xt/table stars_in #{"movie_title" "star_name" "year"}
                                 #xt/table movie_star #{"name" "birthdate"}}})))

  (t/is (=plan-file
         "basic-query-1"
         (sql/plan "FROM stars_in AS si, movie_star AS ms WHERE si.star_name = ms.name AND ms.birthdate = 1960 SELECT si.movie_title"
                   {:table-info {#xt/table stars_in #{"movie_title" "star_name" "year"}
                                 #xt/table movie_star #{"name" "birthdate"}}})))

  (t/is (=plan-file
         "basic-query-2"
         (sql/plan "FROM stars_in AS si, movie_star AS ms WHERE si.star_name = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950 SELECT si.movie_title"
                   {:table-info {#xt/table stars_in #{"movie_title" "star_name" "year"}
                                 #xt/table movie_star #{"name" "birthdate"}}})))

  (t/is (=plan-file
         "basic-query-3"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si, movie_star AS ms WHERE si.star_name = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
                   {:table-info {#xt/table stars_in #{"movie_title" "star_name" "year"}
                                 #xt/table movie_star #{"name" "birthdate"}}})))

  (t/is (=plan-file
         "basic-query-4"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si, (SELECT ms.name FROM movie_star AS ms WHERE ms.birthdate = 1960) AS m WHERE si.star_name = m.name"
                   {:table-info {#xt/table stars_in #{"movie_title" "star_name" "year"}
                                 #xt/table movie_star #{"name" "birthdate"}}})))

  (t/is (=plan-file
         "basic-query-5"
         (sql/plan "SELECT si.movie_title FROM movie AS m JOIN stars_in AS si ON m.title = si.movie_title AND si.`year` = m.movie_year"
                   {:table-info {#xt/table movie #{"title" "movie_year"}
                                 #xt/table stars_in #{"movie_title" "year"}}})))

  (t/is (=plan-file
         "basic-query-6"
         (sql/plan "SELECT si.movie_title FROM movie AS m LEFT JOIN stars_in AS si ON m.title = si.movie_title AND si.`year` = m.movie_year"
                   {:table-info {#xt/table movie #{"title" "movie_year"}
                                 #xt/table stars_in #{"movie_title" "year"}}})))

  (t/is (=plan-file
         "basic-query-9"
         (sql/plan "SELECT me.name, SUM(m.`length`) FROM movie_exec AS me, movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.`year`) < 1930"
                   {:table-info {#xt/table movie_exec #{"name" "cert"}
                                 #xt/table movie #{"producer" "year" "length"}}})))

  (t/is (=plan-file
         "basic-query-10"
         (sql/plan "SELECT SUM(m.`length`) FROM movie AS m"
                   {:table-info {#xt/table movie #{"length"}}})))

  (t/is (=plan-file
         "basic-query-11"
         (sql/plan "SELECT * FROM stars_in AS si(name)"
                   {:table-info {#xt/table stars_in #{"name" "title"}}})))

  (t/is (=plan-file
         "basic-query-11"
         (sql/plan "FROM stars_in AS si(name)"
                   {:table-info {#xt/table stars_in #{"name" "title"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-12"
         (sql/plan "SELECT * FROM (SELECT si.name FROM stars_in AS si) AS foo(bar)"
                   {:table-info {#xt/table stars_in #{"name"}}})))

  (t/is (=plan-file
         "basic-query-12"
         (sql/plan "FROM (SELECT si.name FROM stars_in AS si) AS foo(bar)"
                   {:table-info {#xt/table stars_in #{"name"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-13"
         (sql/plan "SELECT si.* FROM stars_in AS si WHERE si.name = si.lastname"
                   {:table-info {#xt/table stars_in #{"name" "lastname"}}}))))

(t/deftest test-values
  (t/is (=plan-file
         "basic-query-32"
         (sql/plan "VALUES (1, 2), (3, 4)")))

  (t/is (=plan-file
         "basic-query-33"
         (sql/plan "VALUES 1, 2"))))

(t/deftest test-is-null
  (t/is (=plan-file
         "basic-query-35"
         (sql/plan "SELECT * FROM t1 AS t1(a) WHERE t1.a IS NULL"
                   {:table-info {#xt/table t1 #{"a"}}})))

  (t/is (=plan-file
         "basic-query-35"
         (sql/plan "FROM t1 AS t1(a) WHERE t1.a IS NULL"
                   {:table-info {#xt/table t1 #{"a"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-36"
         (sql/plan "SELECT * FROM t1 WHERE a IS NOT NULL"
                   {:table-info {#xt/table t1 #{"a"}}})))

  (t/is (=plan-file
         "basic-query-36"
         (sql/plan "FROM t1 WHERE a IS NOT NULL"
                   {:table-info {#xt/table t1 #{"a"}}}))
        "implicit SELECT *"))

(t/deftest test-case
  (t/is (=plan-file
         "basic-query-34"
         (sql/plan "SELECT CASE t1.a + 1 WHEN t1.b THEN 111 WHEN t1.c THEN 222 WHEN t1.d THEN 333 WHEN t1.e THEN 444 ELSE 555 END,
                    CASE WHEN t1.a < t1.b - 3 THEN 111 WHEN t1.a <= t1.b THEN 222 WHEN t1.a < t1.b+3 THEN 333 ELSE 444 END,
                    CASE t1.a + 1 WHEN t1.b, t1.c THEN 222 WHEN t1.d, t1.e + 1 THEN 444 ELSE 555 END FROM t1"
                   {:table-info {#xt/table t1 #{"a" "b" "c" "d" "e"}}})))

  (t/is (=plan-file
         "basic-query-37"
         (sql/plan "SELECT NULLIF(t1.a, t1.b) FROM t1"
                   {:table-info {#xt/table t1 #{"a" "b"}}}))))

(t/deftest test-basic-set-operators
  (t/is (=plan-file
         "basic-query-14"
         (sql/plan "SELECT DISTINCT si.movie_title FROM stars_in AS si"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-15"
         (sql/plan "SELECT si.name FROM stars_in AS si EXCEPT SELECT si.name FROM stars_in AS si"
                   {:table-info {#xt/table stars_in #{"name"}}})))

  (t/is (=plan-file
         "basic-query-16"
         (sql/plan "SELECT si.name FROM stars_in AS si UNION ALL SELECT si.name FROM stars_in AS si"
                   {:table-info {#xt/table stars_in #{"name"}}})))

  (t/is (=plan-file
         "basic-query-17"
         (sql/plan "SELECT si.name FROM stars_in AS si INTERSECT SELECT si.name FROM stars_in AS si"
                   {:table-info {#xt/table stars_in #{"name"}}})))

  #_ ; TODO should rename the RHS col
  (t/is (=plan-file
         "basic-query-18"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si UNION SELECT si.name FROM stars_in AS si"
                   {:table-info {#xt/table stars_in #{"movie_title" "name"}}})))

  #_ ; TODO order-by over union shouldn't create eobrs
  (t/is (=plan-file
         "basic-query-19"
         (sql/plan "SELECT si.name FROM stars_in AS si UNION SELECT si.name FROM stars_in AS si ORDER BY name"
                   {:table-info {#xt/table stars_in #{"name"}}}))))

(t/deftest test-order-by-limit-offset
  (t/is (=plan-file
         "basic-query-20"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si FETCH FIRST 10 ROWS ONLY"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-20"
         (sql/plan "FROM stars_in AS si SELECT si.movie_title FETCH FIRST 10 ROWS ONLY"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-21"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si OFFSET 5 ROWS"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-21"
         (sql/plan "FROM stars_in AS si SELECT si.movie_title OFFSET 5 ROWS"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-22"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si OFFSET 5 LIMIT 10"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-22"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si LIMIT 10 OFFSET 5"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-22"
         (sql/plan "FROM stars_in AS si SELECT si.movie_title OFFSET 5 LIMIT 10"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-22"
         (sql/plan "FROM stars_in AS si SELECT si.movie_title LIMIT 10 OFFSET 5"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-23"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si ORDER BY si.movie_title"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-24"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si ORDER BY si.movie_title OFFSET 100 ROWS"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-25"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si ORDER BY movie_title DESC"
                   {:table-info {#xt/table stars_in #{"movie_title"}}})))

  #_ ; TODO this is an error
  (t/is (=plan-file
         "basic-query-26"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si ORDER BY si.\"year\" = 'foo' DESC, movie_title"
                   {:table-info {#xt/table stars_in #{"movie_title" "year"}}})))

  (t/is (=plan-file
         "basic-query-27"
         (sql/plan "SELECT si.movie_title FROM stars_in AS si ORDER BY si.`year`"
                   {:table-info {#xt/table stars_in #{"movie_title" "year"}}})))

  (t/is (=plan-file
         "basic-query-28"
         (sql/plan "SELECT si.`year` = 'foo' FROM stars_in AS si ORDER BY si.`year` = 'foo'"
                   {:table-info {#xt/table stars_in #{"year"}}}))))

(t/deftest test-limit-offset-params-3699
  (t/is (=plan-file "test-limit-offset-params-3699"
                    (sql/plan "SELECT * FROM foo OFFSET ? LIMIT ?"
                              {:table-info {#xt/table foo #{"a"}}})))

  (xt/submit-tx tu/*node* ["INSERT INTO foo SELECT _id FROM generate_series(1, 100) ids (_id)"])

  (t/is (= [{:xt/id 21} {:xt/id 22} {:xt/id 23} {:xt/id 24} {:xt/id 25}]
           (xt/q tu/*node* ["SELECT _id FROM foo ORDER BY _id OFFSET ? LIMIT ?" 20 5])))

  (t/is (= [{:xt/id 21} {:xt/id 22} {:xt/id 23} {:xt/id 24} {:xt/id 25}]
           (jdbc/execute! tu/*node* ["SELECT _id FROM foo ORDER BY _id OFFSET ? LIMIT ?" 20 5]
                          {:builder-fn xt-jdbc/builder-fn}))))

(t/deftest test-unnest
  (t/is (=plan-file
         "basic-query-29"
         (sql/plan "SELECT film FROM stars_in AS si, UNNEST(si.films) AS film(film)"
                   {:table-info {#xt/table stars_in #{"films"}}})))

  (t/is (=plan-file
         "basic-query-30"
         (sql/plan "SELECT * FROM stars_in AS si, UNNEST(si.films) AS film"
                   {:table-info {#xt/table stars_in #{"films"}}})))

  (t/is (=plan-file
         "basic-query-30"
         (sql/plan "FROM stars_in si, UNNEST(films) AS film"
                   {:table-info {#xt/table stars_in #{"films"}}}))

        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-31"
         (sql/plan "SELECT * FROM stars_in AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                   {:table-info {#xt/table stars_in #{"films"}}})))

  (t/is (=plan-file
         "basic-query-31"
         (sql/plan "FROM stars_in AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                   {:table-info {#xt/table stars_in #{"films"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "unnest-query-1"
         (sql/plan "SELECT * FROM stars_in AS si, UNNEST(si.films) WITH ORDINALITY AS film(film, film_ord)"
                   {:table-info {#xt/table stars_in #{"films"}}})))

  (xt/submit-tx tu/*node* [[:put-docs :actors
                            {:xt/id "bob", :name "Bob", :films ["The Great Escape", "Bob the Builder", "Bob the Builder Electric Boogaloo"]}
                            {:xt/id "steve", :name "Steve", :films ["Hot Fuzz" "Shaun of the Dead"]}]])

  (t/is (= [{:name "Bob", :film "The Great Escape"}
            {:name "Bob", :film "Bob the Builder"}
            {:name "Bob", :film "Bob the Builder Electric Boogaloo"}
            {:name "Steve", :film "Hot Fuzz"}
            {:name "Steve", :film "Shaun of the Dead"}]
           (xt/q tu/*node* "SELECT name, film FROM actors, UNNEST(films) AS films(film)")))

  (t/is (= [{:name "Bob", :film "The Great Escape", :ord 1}
            {:name "Bob", :film "Bob the Builder", :ord 2}
            {:name "Bob", :film "Bob the Builder Electric Boogaloo", :ord 3}
            {:name "Steve", :film "Hot Fuzz", :ord 1}
            {:name "Steve", :film "Shaun of the Dead", :ord 2}]
           (xt/q tu/*node* "SELECT name, film, ord FROM actors, UNNEST(films) WITH ORDINALITY AS films(film, ord)"))))

(t/deftest test-unnest-null-or-missing-4075
  (xt/execute-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, a: 2}"])
  (t/is (= [] (xt/q tu/*node* "SELECT b FROM foo, UNNEST(foo.b) AS bs(b)"))))

(t/deftest test-cross-join
  (t/is (=plan-file
         "cross-join-1"
         (sql/plan "SELECT * FROM a CROSS JOIN b"
                   {:table-info {#xt/table a #{"a1" "a2"}
                                 #xt/table b #{"b1"}}})))
  (t/is (=plan-file
         "cross-join-2"
         (sql/plan "SELECT c1 FROM a, b CROSS JOIN c"
                   {:table-info {#xt/table a #{"a1" "a2"}
                                 #xt/table b #{"b1"}
                                 #xt/table c #{"c1"}}}))))

(t/deftest test-named-columns-join
  (t/is (=plan-file
         "basic-query-7"
         (sql/plan "SELECT si.title FROM movie AS m JOIN stars_in AS si USING (title)"
                   {:table-info {#xt/table movie #{"title"}
                                 #xt/table stars_in #{"title"}}})))

  (t/is (=plan-file
         "basic-query-8"
         (sql/plan "SELECT si.title FROM movie AS m LEFT OUTER JOIN stars_in AS si USING (title)"
                   {:table-info {#xt/table movie #{"title"}
                                 #xt/table stars_in #{"title"}}}))))

(t/deftest test-natural-join
  (t/is (=plan-file
         "natural-join-1"
         (sql/plan "SELECT si.title, m.`length`, si.films FROM movie AS m NATURAL JOIN stars_in AS si"
                   {:table-info {#xt/table movie #{"title" "length"}
                                 #xt/table stars_in #{"title" "films"}}})))

  (t/is (=plan-file
         "natural-join-2"
         (sql/plan "SELECT si.title, m.`length`, si.films FROM movie AS m NATURAL RIGHT OUTER JOIN stars_in AS si"
                   {:table-info {#xt/table movie #{"title" "length"}
                                 #xt/table stars_in #{"title" "films"}}}))))


;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(t/deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (=plan-file
           "scalar-subquery-in-select-1"
           (sql/plan "SELECT (1 = (SELECT bar FROM foo)) AS some_column FROM x WHERE y = 1"
                     {:table-info {#xt/table x #{"y"}
                                   #xt/table foo #{"bar"}}})))

    (t/is (=plan-file
           "scalar-subquery-in-select-2"
           (sql/plan "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
                     {:table-info {#xt/table x #{"y"}
                                   #xt/table foo #{"bar"}}}))))

  (t/testing "Scalar subquery in WHERE"
    (t/is (=plan-file
           "scalar-subquery-in-where"
           (sql/plan "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"
                     {:table-info {#xt/table x #{"y"}
                                   #xt/table foo #{"bar"}}}))))

  (t/testing "Correlated scalar subquery in SELECT"
    (t/is (=plan-file
           "correlated-scalar-subquery-in-select"
           (sql/plan "SELECT (1 = (SELECT bar = z FROM foo)) AS some_column FROM x WHERE y = 1"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table foo #{"bar"}}}))))

  (t/testing "EXISTS in WHERE"
    (t/is (=plan-file
           "exists-in-where"
           (sql/plan "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10.0"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "EXISTS as expression in SELECT"
    (t/is (=plan-file
           "exists-as-expression-in-select"
           (sql/plan "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "NOT EXISTS in WHERE"
    (t/is (=plan-file
           "not-exists-in-where"
           (sql/plan "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "IN in WHERE"
    (t/is (=plan-file
           "in-in-where-select"
           (sql/plan "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}})))

    (t/is (=plan-file
           "in-in-where-set"
           (sql/plan "SELECT x.y FROM x WHERE x.z IN (1, 2)"
                     {:table-info {#xt/table x #{"y" "z"}}}))))

  (t/testing "NOT IN in WHERE"
    (t/is (=plan-file
           "not-in-in-where"
           (sql/plan "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "ALL in WHERE"
    (t/is (=plan-file
           "all-in-where"
           (sql/plan "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "ANY in WHERE"
    (t/is (=plan-file
           "any-in-where"
           (sql/plan "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "ALL as expression in SELECT"
    (t/is (=plan-file
           "all-as-expression-in-select"
           (sql/plan "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"
                     {:table-info {#xt/table x #{"y" "z"}
                                   #xt/table y #{"z"}}}))))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (t/is (=plan-file
           "decorrelation-1"
           (sql/plan "SELECT c.custkey FROM customer c
                      WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)"
                     {:table-info {#xt/table customer #{"custkey"}
                                   #xt/table orders #{"custkey" "totalprice"}}})))

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (t/is (=plan-file
           "decorrelation-2"
           (sql/plan "SELECT * FROM customers AS customers(country, custno)
                      WHERE customers.country = 'Mexico' AND
                      EXISTS (SELECT * FROM orders AS orders(custno) WHERE customers.custno = orders.custno)"
                     {:table-info {#xt/table customers #{"country" "custno"}
                                   #xt/table orders #{"custno"}}})))

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (t/is (=plan-file
           "decorrelation-3"
           (sql/plan "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
                      FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)"
                     {:table-info {#xt/table customers #{"name" "custno" "country"}
                                   #xt/table orders #{"custno"}
                                   #xt/table salesp #{"country"}}})))

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (t/is (=plan-file
           "decorrelation-4"
           (sql/plan "SELECT s.name, e.course
                      FROM students s, exams e
                      WHERE s.id = e.sid AND
                      e.grade = (SELECT MIN(e2.grade)
                      FROM exams e2
                      WHERE s.id = e2.sid)"
                     {:table-info {#xt/table students #{"id" "name"}
                                   #xt/table exams #{"sid" "grade" "course"}}})))

    (t/is (=plan-file
           "decorrelation-5"
           (sql/plan
            "SELECT s.name, e.course
              FROM students s, exams e
              WHERE s.id = e.sid AND
              (s.major = 'CS' OR s.major = 'Games Eng') AND
              e.grade >= (SELECT AVG(e2.grade) + 1
              FROM exams e2
              WHERE s.id = e2.sid OR
              (e2.curriculum = s.major AND
              s.\"year\" > e2.\"date\"))"
            {:table-info {#xt/table students #{"id" "major" "name" "year"}
                          #xt/table exams #{"sid" "grade" "course" "curriculum" "date"}}})))


    (t/testing "Subqueries in join conditions"

      (->> "uncorrelated subquery"
           (t/is (=plan-file
                  "subquery-in-join-uncorrelated-subquery"
                  (sql/plan "select foo.a from foo join bar on bar.c = (select foo.b from foo)"
                            {:table-info {#xt/table foo #{"a" "b"}
                                          #xt/table bar #{"c"}}}))))

      (->> "correlated subquery"
           (t/is (=plan-file
                  "subquery-in-join-correlated-subquery"
                  (sql/plan "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"
                            {:table-info {#xt/table foo #{"a" "b"}
                                          #xt/table bar #{"b" "c"}}}))))

      ;; TODO unable to decorr, need to be able to pull the select over the max-1-row
      ;; although should be able to do this now, no such thing as max-1-row any more
      (->> "correlated equalty subquery"
           (t/is (=plan-file
                  "subquery-in-join-correlated-equality-subquery"
                  (sql/plan "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)"
                            {:table-info {#xt/table foo #{"a" "b"}
                                          #xt/table bar #{"b" "c"}}})))))))

(t/deftest lateral-derived-table
  (t/is (=plan-file
         "lateral-derived-table-1"
         (sql/plan "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
                   {:table-info {#xt/table x #{"y"}
                                 #xt/table z #{"z"}}})))

  (t/is (=plan-file
         "lateral-derived-table-2"
         (sql/plan "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"
                   {:table-info {#xt/table z #{"z"}}}))))

(t/deftest join-lateral
  (t/is (=plan-file
         "join-lateral-1"
         (sql/plan "SELECT x.y, y.z FROM x CROSS JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
                   {:table-info {#xt/table x #{"y"}
                                 #xt/table z #{"z"}}})))

  (t/is (=plan-file
         "join-lateral-2"
         (sql/plan "SELECT x.y, y.z FROM x LEFT JOIN LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y ON TRUE"
                   {:table-info {#xt/table x #{"y"}
                                 #xt/table z #{"z"}}})))

  (t/is (=plan-file
         "join-lateral-3"
         (sql/plan "SELECT x, x.y, y.z FROM x NATURAL JOIN LATERAL (SELECT x, z.z FROM z WHERE z.z = x.y) AS y"
                   {:table-info {#xt/table x #{"x" "y"}
                                 #xt/table z #{"x" "z"}}})))

  (xt/submit-tx tu/*node* [[:put-docs :nested-table {:xt/id 1}]])
  (xt/submit-tx tu/*node* [[:put-docs :other-table {:xt/id 1} {:xt/id 2} {:xt/id 3}]])

  (t/is (= [{:nid 1, :oid 1}]
           (xt/q tu/*node* "SELECT nt._id nid, o._id oid
                            FROM nested_table nt
                              CROSS JOIN LATERAL (SELECT * FROM other_table o WHERE o._id = nt._id) o"))))

(t/deftest test-qc-array-expr-3539
  (t/is (=plan-file "test-qc-array-expr"
                    (sql/plan "SELECT * FROM foo WHERE foo.a = ANY(CURRENT_SCHEMAS(true))"
                              {:table-info {#xt/table foo #{"a"}}})))

  (xt/submit-tx tu/*node* [[:put-docs :foo
                            {:xt/id 1, :a "pg_catalog"}
                            {:xt/id 2, :a "public"}
                            {:xt/id 3, :a "foo"}]])

  (t/is (= [{:xt/id 2, :a "public"}]
           (xt/q tu/*node* "SELECT * FROM foo WHERE a = ANY(CURRENT_SCHEMAS(false))")))

  (t/is (= [{:xt/id 2, :a "public"} {:xt/id 1, :a "pg_catalog"}]
           (xt/q tu/*node* "SELECT * FROM foo WHERE a = ANY(CURRENT_SCHEMAS(true))"))))

(t/deftest test-current-catalog
  (t/is (= [{:cat "xtdb"}]
           (xt/q tu/*node* "SELECT CURRENT_CATALOG cat")))

  (t/is (= [{:cat "xtdb"}]
           (xt/q tu/*node* "SELECT CURRENT_CATALOG() cat"))))

(t/deftest test-in-subquery
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 1 :foo "Hello"}]
                           [:put-docs :docs {:xt/id 2 :x 2 :y 1}]])

  (t/is (= [{:xt/id 1, :foo "Hello", :x 1}]
           (xt/q tu/*node* "SELECT * FROM docs AS d1 WHERE d1.x IN (SELECT d2.y FROM docs AS d2 WHERE d2.y = d1.x)"))))

(t/deftest parameters-referenced-in-relation-test
  (t/are [expected plan apply-columns]
      (= expected (lp/parameters-referenced-in-relation? plan (vals apply-columns)))
    true '[:table [{x6 ?x8}]] '{x2 ?x8}
    false '[:table [{x6 ?x4}]] '{x2 ?x8}))

(t/deftest non-semi-join-subquery-optimizations-test
  (t/is (=plan-file
         "non-semi-join-subquery-optimizations-test-1"
         (sql/plan "select f.a from foo f where f.a in (1,2) or f.b = 42"
                   {:table-info {#xt/table foo #{"a" "b"}}}))
        "should not be decorrelated")
  (t/is (=plan-file
         "non-semi-join-subquery-optimizations-test-2"
         (sql/plan "select f.a from foo f where true = (EXISTS (SELECT foo.c from foo))"
                   {:table-info {#xt/table foo #{"a" "c"}}}))
        "should be decorrelated as a cross join, not a semi/anti join"))

(t/deftest multiple-ins-in-where-clause
  (t/is (=plan-file
         "multiple-ins-in-where-clause"
         (sql/plan "select f.a from foo f where f.a in (1,2) AND f.a = 42 AND f.b in (3,4)"
                   {:table-info {#xt/table foo #{"a" "b"}}}))))

#_ ; FIXME broken
(t/deftest deeply-nested-correlated-query
  (t/is (=plan-file
         "deeply-nested-correlated-query"
         (sql/plan "SELECT R1.A, R1.B
                    FROM R R1, S
                    WHERE EXISTS
                    (SELECT R2.A, R2.B
                    FROM R R2
                    WHERE R2.A = R1.B AND EXISTS
                    (SELECT R3.A, R3.B
                    FROM R R3
                    WHERE R3.A = R2.B AND R3.B = S.C))"
                   {:table-info {#xt/table r #{"a" "b"}, #xt/table s #{"c"}}}))))

(t/deftest test-array-element-reference-107
  (t/is (=plan-file
         "test-array-element-reference-107-1"
         (sql/plan "SELECT u.a[1] AS first_el FROM u"
                   {:table-info {#xt/table u #{"a"}}})))

  (t/is (=plan-file
         "test-array-element-reference-107-2"
         (sql/plan "SELECT u.b[u.a[1]] AS dyn_idx FROM u"
                   {:table-info {#xt/table u #{"a" "b"}}}))))

(t/deftest test-current-time-111
  (t/is (=plan-file
         "test-current-time-111"
         (sql/plan "
                    SELECT u.a,
                    CURRENT_TIME, CURRENT_TIME(2),
                    CURRENT_DATE,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(4),
                    LOCALTIME, LOCALTIME(6),
                    LOCALTIMESTAMP, LOCALTIMESTAMP(9)
                    FROM u"
                   {:table-info {#xt/table u #{"a"}}}))))

(t/deftest test-dynamic-parameters-103
  (t/is (=plan-file
         "test-dynamic-parameters-103-1"
         (sql/plan "SELECT foo.a FROM foo WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {#xt/table foo #{"a" "b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-2"
         (sql/plan "SELECT foo.a
                    FROM foo, (SELECT bar.b FROM bar WHERE bar.c = ?) bar (b)
                    WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {#xt/table foo #{"a" "b" "c"}
                                 #xt/table bar #{"b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-subquery-project"
         (sql/plan "SELECT t1.col1, (SELECT ? FROM bar WHERE bar.col1 = 4) FROM t1"
                   {:table-info {#xt/table t1 #{"col1"}
                                 #xt/table bar #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-top-level-project"
         (sql/plan "SELECT t1.col1, ? FROM t1"
                   {:table-info {#xt/table t1 #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-set-value"
         (sql/plan "UPDATE t1 SET col1 = ?"
                   {:table-info {#xt/table t1 #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-table-values"
         (sql/plan "SELECT bar.foo FROM (VALUES (?)) AS bar(foo)"
                   {:table-info {#xt/table bar #{"foo"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-app-time"
         (sql/plan "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                   {:table-info {#xt/table users #{"first_name" "id"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-app-time"
         (sql/plan "UPDATE users FOR VALID_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                   {:table-info {#xt/table users #{"first_name" "id"}}}))))

(t/deftest from-where-param-order-bug-4305
  (t/is (=plan-file
         "from-where-param-order-bug-4305"
         (sql/plan "SELECT _id FROM foo FOR VALID_TIME AS OF ? WHERE _id = ?"
                   {:table-info {#xt/table foo #{"_id"}}}))))

(t/deftest test-dynamic-temporal-filters-3068
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of"
      (sql/plan "SELECT bar FROM foo FOR VALID_TIME AS OF ?"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-from-to"
      (sql/plan "SELECT bar FROM foo FOR VALID_TIME FROM ? TO ?"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-between"
      (sql/plan "SELECT bar FROM foo FOR VALID_TIME BETWEEN ? AND ?"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "AS OF SYSTEM TIME"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of-system-time"
      (sql/plan "SELECT bar FROM foo FOR SYSTEM_TIME AS OF ?"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "using dynamic AS OF in a query"
    (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015"}
                              {:xt/id :matthew}]
                             [:put-docs {:into :docs, :valid-from #inst "2018"}
                              {:xt/id :mark}]])
    (t/is (= [{:xt/id :matthew}]
             (xt/q tu/*node* ["SELECT docs._id FROM docs FOR VALID_TIME AS OF ?" #inst "2016"])))

    (t/is (= [{:xt/id :matthew}]
             (jdbc/execute! tu/*node* ["SELECT docs._id FROM docs FOR VALID_TIME AS OF ?" #inst "2016"]
                            {:builder-fn xt-jdbc/builder-fn})))))

(t/deftest test-order-by-null-handling-159
  (t/is (=plan-file
         "test-order-by-null-handling-159-1"
         (sql/plan "SELECT a FROM foo ORDER BY a NULLS FIRST"
                   {:table-info {#xt/table foo #{"a"}}})))

  (t/is (=plan-file
         "test-order-by-null-handling-159-2"
         (sql/plan "SELECT a FROM foo ORDER BY a NULLS LAST"
                   {:table-info {#xt/table foo #{"a"}}}))))

(t/deftest test-arrow-table
  (t/is (=plan-file
         "test-arrow-table-1"
         (sql/plan "SELECT foo.a FROM ARROW_TABLE('test.arrow') AS foo (a)")))

  (t/is (=plan-file
         "test-arrow-table-2"
         (sql/plan "SELECT * FROM ARROW_TABLE('test.arrow') AS foo (a, b)"))))

(t/deftest test-projects-that-matter-are-maintained
  (t/is (=plan-file
         "projects-that-matter-are-maintained"
         (sql/plan
          "SELECT customers.id
            FROM customers
            UNION
            SELECT o.id
            FROM
            (SELECT orders.id, orders.product
            FROM orders) AS o"
          {:table-info {#xt/table customers #{"id"}
                        #xt/table orders #{"id" "product"}}}))))

(t/deftest test-semi-and-anti-joins-are-pushed-down
  (t/is (=plan-file
         "test-semi-and-anti-joins-are-pushed-down"
         (sql/plan
          "SELECT t1.a1
            FROM t1, t2, t3
            WHERE t1.b1 in (532,593)
            AND t2.b1 in (808,662)
            AND t3.c1 in (792,14)
            AND t1.a1 = t2.a2"
          {:table-info {#xt/table t1 #{"a1" "b1"}
                        #xt/table t2 #{"b1" "a2"}
                        #xt/table t3 #{"c1"}}}))))

(t/deftest datascript-test-aggregates
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id :cerberus, :heads 3}]
                           [:put-docs :docs {:xt/id :medusa, :heads 1}]
                           [:put-docs :docs {:xt/id :cyclops, :heads 1}]
                           [:put-docs :docs {:xt/id :chimera, :heads 1}]])]
    (t/is (= #{{:heads 1, :count-heads 3} {:heads 3, :count-heads 1}}
             (set (xt/q tu/*node*
                        "SELECT heads, COUNT(heads) AS count_heads FROM docs")))
          "head frequency")

    (t/is (= #{{:sum-heads 6, :min-heads 1, :max-heads 3, :count-heads 4}}
             (set (xt/q tu/*node*
                        "SELECT SUM(heads) AS sum_heads, MIN(heads) AS min_heads, MAX(heads) AS max_heads, COUNT(heads) AS count_heads FROM docs")))
          "various aggs")

    (t/is (= #{{:or-medusa true, :and-medusa false, :every-medusa false}}
             (set (xt/q tu/*node*
                        "SELECT BOOL_OR(_id = 'medusa') AS or_medusa, BOOL_AND(_id = 'medusa') AS and_medusa, EVERY(_id = 'medusa') AS every_medusa FROM docs")))
          "bool aggs")

    (t/is (= #{{:heads 1, :count-heads 3}}
             (set (xt/q tu/*node*
                        "SELECT heads, COUNT(heads) AS count_heads FROM docs HAVING COUNT(heads) > 1")))
          "having frequency > 1")

    (t/is (= #{{:heads 1, :count-heads 3}}
             (set (xt/q tu/*node*
                        "FROM docs HAVING COUNT(heads) > 1 SELECT heads, COUNT(heads) AS count_heads")))
          "having frequency > 1")

    #_ ; this is a Postgres extension to the SQL syntax that we may want to support at a later date.
    (t/is (= #{{:heads 1, :count-heads 3}}
             (set (xt/q tu/*node*
                        "SELECT heads, COUNT(heads) AS count_heads FROM docs HAVING count_heads > 1")))
          "having referencing select column")))

(t/deftest test-group-by-with-projected-column-in-expr
  (t/is (=plan-file
         "test-group-by-with-projected-column-in-expr"
         (sql/plan
          "SELECT foo.a - 4 AS bar
            FROM foo
            GROUP BY foo.a"
          {:table-info {#xt/table foo #{"a"}}})))

  (t/is (=plan-file
         "test-group-by-with-projected-column-in-expr-2"
         (sql/plan
          "SELECT SUM(foo.a - 4) AS bar
            FROM foo "
          {:table-info {#xt/table foo #{"a"}}}))))

(t/deftest test-window-functions
  (t/is (=plan-file
         "test-window-with-partition-and-order-by"
         (sql/plan "SELECT y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) FROM docs"
                   {:table-info {#xt/table docs #{"y" "z"}}})))

  (t/is (thrown-with-msg? UnsupportedOperationException #"TODO"
                          (sql/plan "SELECT ROW_NUMBER() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM docs"
                                    {:table-info {#xt/table docs #{"y" "z"}}}))
        "window frames not yet supported")

  (t/is (thrown-with-msg? UnsupportedOperationException #"TODO"
                          (sql/plan "SELECT
                                       ROW_NUMBER() OVER (PARTITION BY y) AS rn,
                                       ROW_NUMBER() OVER (PARTITION BY z) AS rn2
                                     FROM docs"
                                    {:table-info {#xt/table docs #{"y" "z"}}}))
        "multiple windows not supported")

  ;; TODO similar to #3640
  #_
  (t/is (= nil
           (sql/plan "SELECT ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) FROM docs"
                     {:table-info {#xt/table docs #{"_id"}}})))

  (let [docs [{:a 1 :b 20}
              {:a 1 :b 10}
              {:a 2 :b 30}
              {:a 2 :b 40}
              {:a 1 :b 50}
              {:a 1 :b 60}
              {:a 2 :b 70}
              {:a 3 :b 80}
              {:a 3 :b 90}]]

    (xt/submit-tx tu/*node* [(into [:put-docs :docs ]
                                   (for [[id doc] (zipmap (range) docs)] (assoc doc :xt/id id)))])

    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 60, :rn 4}
               {:a 2, :b 30, :rn 1}
               {:a 2, :b 40, :rn 2}
               {:a 2, :b 70, :rn 3}
               {:a 3, :b 80, :rn 1}
               {:a 3, :b 90, :rn 2}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM docs")
                  set)))

    (t/is (= #{{:a 1, :b 60, :rn 1}
               {:a 1, :b 10, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 20, :rn 4}
               {:a 2, :b 70, :rn 1}
               {:a 2, :b 30, :rn 2}
               {:a 2, :b 40, :rn 3}
               {:a 3, :b 90, :rn 1}
               {:a 3, :b 80, :rn 2}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a) AS rn FROM docs")
                  set))
          "only partition by")

    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 2, :b 30, :rn 3}
               {:a 2, :b 40, :rn 4}
               {:a 1, :b 50, :rn 5}
               {:a 1, :b 60, :rn 6}
               {:a 2, :b 70, :rn 7}
               {:a 3, :b 80, :rn 8}
               {:a 3, :b 90, :rn 9}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (ORDER BY b) AS rn FROM docs")
                  set))
          "only order by")

    (t/is (= #{{:a 1, :b 60, :rn 1}
               {:a 2, :b 70, :rn 2}
               {:a 2, :b 30, :rn 3}
               {:a 3, :b 90, :rn 4}
               {:a 1, :b 10, :rn 5}
               {:a 3, :b 80, :rn 6}
               {:a 1, :b 50, :rn 7}
               {:a 2, :b 40, :rn 8}
               {:a 1, :b 20, :rn 9}}
             (-> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER () AS rn FROM docs")
                 set))
          "nothing")
    #_
    (t/is (= [{:a 1, :b 60, :rn 1}
              {:a 2, :b 70, :rn 2}
              {:a 2, :b 30, :rn 3}
              {:a 3, :b 90, :rn 4}
              {:a 1, :b 10, :rn 5}
              {:a 3, :b 80, :rn 6}
              {:a 1, :b 50, :rn 7}
              {:a 2, :b 40, :rn 8}
              {:a 1, :b 20, :rn 9}]
             (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) AS rn FROM docs"))
          "no existing columns")))

(t/deftest test-operators-with-unresolved-column-references-3640
  (t/is (=plan-file
         "test-group-by-with-unresolved-column-reference"
         (sql/plan "SELECT SUM(_id + 1) FROM docs GROUP BY x"
                   {:table-info {#xt/table docs #{"_id"}}})))

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

  (t/is (= [{:s 1}]
           (xt/q tu/*node* "SELECT SUM(_id) AS s FROM docs GROUP BY x")))

  (t/is (=plan-file
         "test-having-with-unresolved-column-reference"
         (sql/plan "SELECT SUM(_id) AS s FROM docs HAVING SUM(x) > 1"
                   {:table-info {#xt/table docs #{"_id"}}})))

  (t/is (= []
           (xt/q tu/*node* "SELECT SUM(_id) AS s FROM docs HAVING SUM(x) > 1")))

  (t/is (=plan-file
         "test-where-with-unresolved-column-reference"
         (sql/plan "SELECT _id AS s FROM docs WHERE x > 1"
                   {:table-info {#xt/table docs #{"_id"}}})))

  (t/is (= []
           (xt/q tu/*node* "SELECT _id AS s FROM docs WHERE x > 1"))))

(t/deftest test-array-subqueries
  (t/are [file q]
      (=plan-file file (sql/plan q {:table-info {#xt/table a #{"a" "b"}, #xt/table b #{"b1" "b2"}}}))

    "test-array-subquery1" "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42"
    "test-array-subquery2" "SELECT ARRAY(select b.b1 from b where b.b2 = a.b) FROM a where a.a = 42")

  (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id :a1, :a 42, :b 42}]
                           [:put-docs :b
                            {:xt/id :b1, :b1 42, :b2 42}
                            {:xt/id :b2, :b1 43, :b2 43}]])

  (t/is (= [{:xt/column-1 [42 43]}]
           (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b) FROM a where a.a = 42")))

  (t/is (= [{:xt/column-1 [42]}]
           (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42")))

  (t/is (= [{:xt/column-1 [42]}]
           (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = a.b) FROM a where a.a = 42")))

  (t/is (= [{:xt/column-1 []}]
           (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = a.b and b.b2 = 43) FROM a where a.a = 42"))))

(t/deftest test-empty-array-3818
  (t/is (= [{:xt/column-1 []}]
           (xt/q tu/*node* "SELECT ARRAY(select 1 where false)"))
        "select array over empty relation return empty array"))

(t/deftest test-complex-empty-array
  (let [result (xt/q tu/*node* "SELECT ARRAY (
                                     SELECT a.atttypid FROM pg_attribute AS a
                                       WHERE a.attrelid = t.typrelid) as arr
                                   FROM pg_type AS t")]
    (doseq [r result]
      (t/is (= {:arr []} r)))))

(t/deftest test-expr-in-equi-join
  (t/is
   (=plan-file
    "test-expr-in-equi-join-1"
    (sql/plan "SELECT a FROM a JOIN bar b ON a+1 = b+1"
              {:table-info {#xt/table a #{"a"}, #xt/table bar #{"b"}}})))
  (t/is
   (=plan-file
    "test-expr-in-equi-join-2"
    (sql/plan "SELECT a FROM a JOIN bar b ON a = b+1"
              {:table-info {#xt/table a #{"a"}, #xt/table bar #{"b"}}}))))

(t/deftest push-semi-and-anti-joins-down-test
  ;;semi-join was previously been pushed down below the cross join
  ;;where the cols it required weren't in scope
  ;; TODO I think this should be decorr'able?
  (t/is
   (=plan-file
    "push-semi-and-anti-joins-down"
    (sql/plan "SELECT x.foo
               FROM x, y
               WHERE EXISTS (
                       SELECT z.bar
                       FROM z
                       WHERE z.bar = x.foo AND z.baz = y.biz
                     )"
              {:table-info {#xt/table x #{"foo"}
                            #xt/table y #{"biz"}
                            #xt/table z #{"bar" "baz"}}}))))

(t/deftest test-system-time-queries
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "system-time-as-of"
      (sql/plan "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {#xt/table foo #{"bar"}}})))

    (t/is
     (=plan-file
      "system-time-as-of"
      (sql/plan "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "system-time-from-a-to-b"
      (sql/plan "SELECT foo.bar FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "system-time-between-subquery"
      (sql/plan "SELECT (SELECT 4 FROM t1 FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00') FROM t2"
                {:table-info {#xt/table t1 #{}, #xt/table t2 #{}}}))))

  (t/is
   (=plan-file
    "system-time-between-lateraly-derived-table"
    (sql/plan "SELECT x.y, y.z FROM x FOR SYSTEM_TIME AS OF DATE '3001-01-01',
                  LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y"
              {:table-info {#xt/table z #{"z"}, #xt/table x #{"y"}}}))))

(t/deftest setting-default-system-time
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "system-time-as-of"
      (sql/plan "SETTING DEFAULT SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00' SELECT foo.bar FROM foo"
                {:table-info {#xt/table foo #{"bar"}}})))

    (t/is
     (=plan-file
      "system-time-as-of"
      (sql/plan "SETTING DEFAULT SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00' SELECT foo.bar FROM foo"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "system-time-from-a-to-b"
      (sql/plan "SETTING DEFAULT SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00' SELECT foo.bar FROM foo"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/is
   (=plan-file
    "system-time-between-lateraly-derived-table"
    (sql/plan "SETTING DEFAULT SYSTEM_TIME AS OF DATE '3001-01-01'
                 SELECT x.y, y.z
                 FROM x, LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y"
              {:table-info {#xt/table z #{"z"}, #xt/table x #{"y"}}}))))

(t/deftest test-valid-time-period-spec-queries
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "valid-time-period-spec-as-of"
      (sql/plan "SELECT bar FROM foo FOR VALID_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "valid-time-period-spec-from-to"
      (sql/plan "SELECT bar FROM foo FOR VALID_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
                {:table-info {#xt/table foo #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-time-period-spec-between"
      (sql/plan "SELECT 4 FROM t1 FOR VALID_TIME BETWEEN TIMESTAMP '3000-01-01 00:00:00+00:00' AND DATE '3001-01-01'"
                {:table-info {#xt/table t1 #{}}})))))

(t/deftest test-valid-and-system-time-period-spec-queries
  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (sql/plan "SELECT 4
               FROM t1
                  FOR SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
                  FOR VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'"
              {:table-info {#xt/table t1 #{}}})))

  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (sql/plan "SETTING DEFAULT VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01',
                       DEFAULT SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
               SELECT 4 FROM t1"
              {:table-info {#xt/table t1 #{}}})))

  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (sql/plan "SETTING DEFAULT SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01',
                       DEFAULT VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'
               SELECT 4 FROM t1"
              {:table-info {#xt/table t1 #{}}}))))

(t/deftest test-multiple-references-to-temporal-cols
  (t/is
   (=plan-file
    "multiple-references-to-temporal-cols"
    (sql/plan "SELECT foo._valid_from, foo._valid_to, foo._system_from, foo._system_to
                FROM foo FOR SYSTEM_TIME FROM DATE '2001-01-01' TO DATE '2002-01-01'
                WHERE foo._valid_from = 4 AND foo._valid_to > 10
                AND foo._system_from = 20 AND foo._system_to <= 23
                AND foo._VALID_TIME OVERLAPS PERIOD (DATE '2000-01-01', DATE '2004-01-01')"
              {:table-info {#xt/table foo {}}}))))

(t/deftest test-sql-insert-plan
  (t/is (=plan-file "test-sql-insert-plan-1"
                    (sql/plan "INSERT INTO users (_id, name, _valid_from) VALUES (?, ?, ?)")))

  (t/is (=plan-file "test-sql-insert-plan-2"
                    (sql/plan "INSERT INTO users
                               SELECT bar._id, bar.name, bar._valid_from
                               FROM (VALUES (?, ?, ?)) AS bar(_id, name, _valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-3"
                    (sql/plan "INSERT INTO users (_id, name, _valid_from)
                               SELECT bar._id, bar.name, bar._valid_from
                               FROM (VALUES (?, ?, ?)) AS bar(_id, name, _valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-309"
                    (sql/plan "INSERT INTO customer (_id, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        "#309")

  (t/is (=plan-file "test-sql-insert-plan-398"
                    (sql/plan "INSERT INTO foo (_id, _valid_from) VALUES ('foo', DATE '2018-01-01')"))))

(t/deftest test-sql-delete-plan
  (t/is (=plan-file "test-sql-delete-plan"
                    (sql/plan "DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' AS u WHERE u.id = ?"
                              {:table-info {#xt/table users #{"id"}}})))

  (t/is (=plan-file "test-sql-delete-plan"
                    (sql/plan "DELETE FROM users FOR VALID_TIME FROM DATE '2020-05-01' AS u WHERE u.id = ?"
                              {:table-info {#xt/table users #{"id"}}}))))

(t/deftest test-sql-erase-plan
  (t/is (=plan-file "test-sql-erase-plan"
                    (sql/plan "ERASE FROM users AS u WHERE u.id = ?"
                              {:table-info {#xt/table users #{"id"}}}))))

(t/deftest test-sql-update-plan
  (t/is (=plan-file "test-sql-update-plan"
                    (sql/plan "UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' AS u SET first_name = 'Sue' WHERE u.id = ?"
                              {:table-info {#xt/table users #{"id" "first_name" "last_name"}}})))

  (t/is (=plan-file "test-sql-update-plan-with-column-references"
                    (sql/plan "UPDATE foo FOR ALL VALID_TIME SET bar = foo.baz"
                              {:table-info {#xt/table foo #{"bar" "baz" "quux"}}})))

  (t/is (=plan-file "test-sql-update-plan-with-period-references"
                    (sql/plan "UPDATE foo FOR ALL VALID_TIME SET bar = (foo._SYSTEM_TIME OVERLAPS foo._VALID_TIME)"
                              {:table-info {#xt/table foo #{"bar" "baz"}}}))))

(t/deftest dml-target-table-aliases
  (let [opts {:table-info {#xt/table t1 #{"col1"}}}]
    (t/testing "UPDATE"
      (t/is (=plan-file "update-target-table-aliases-1"
                        (sql/plan "UPDATE t1 AS u SET col1 = 30" opts)))

      (t/is (=plan-file "update-target-table-aliases-1"
                        (sql/plan "UPDATE t1 u SET col1 = 30" opts)))

      (t/is (=plan-file "update-target-table-aliases-2"
                        (sql/plan "UPDATE t1 SET col1 = 30" opts))))

    (t/testing "DELETE"
      (t/is (=plan-file "delete-target-table-aliases-1"
                        (sql/plan "DELETE FROM t1 AS u WHERE u.col1 = 30" opts)))

      (t/is (=plan-file "delete-target-table-aliases-1"
                        (sql/plan "DELETE FROM t1 u WHERE u.col1 = 30" opts)))

      (t/is (=plan-file "delete-target-table-aliases-2"
                        (sql/plan "DELETE FROM t1 WHERE t1.col1 = 30" opts))))))

(t/deftest test-system-time-period-predicate
  (t/is
   (=plan-file
    "test-system-time-period-predicate-full-plan"
    (sql/plan
     "SELECT foo.name foo_name, bar.name bar_name
        FROM foo, bar
        WHERE foo._SYSTEM_TIME OVERLAPS bar._SYSTEM_TIME"
     {:table-info {#xt/table foo #{"name"}
                   #xt/table bar #{"name"}}}))))

(t/deftest test-valid-time-correlated-subquery
  (t/is (=plan-file
         "test-valid-time-correlated-subquery-where"
         (sql/plan "SELECT (SELECT foo.name
                    FROM foo
                    WHERE foo._VALID_TIME OVERLAPS bar._VALID_TIME) FROM bar"
                   {:table-info {#xt/table foo #{"name"}
                                 #xt/table bar #{}}})))

  (t/is (=plan-file
         "test-valid-time-correlated-subquery-projection"
         (sql/plan "SELECT (SELECT (foo._VALID_TIME OVERLAPS bar._VALID_TIME) FROM foo)
                    FROM bar"
                   {:table-info {#xt/table foo #{"name"}
                                 #xt/table bar #{}}}))))

(t/deftest test-derived-columns-with-periods
  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-predicate"
    (sql/plan
     "SELECT f._VALID_TIME OVERLAPS f._SYSTEM_TIME
        FROM foo
        AS f (_system_from, _system_to, _valid_from, _valid_to)"
     {:table-info {#xt/table foo #{}}})))

  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-specs"
    (sql/plan
     "SELECT f.bar
        FROM foo
        FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
        FOR VALID_TIME AS OF CURRENT_TIMESTAMP
        AS f (bar)"
     {:table-info {#xt/table foo #{"bar"}}}))))

(t/deftest test-for-all-valid-time-387
  (t/is (=plan-file
         "test-for-all-valid-time-387-query"
         (sql/plan "SELECT bar FROM foo FOR ALL VALID_TIME"
                   {:table-info {#xt/table foo #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-query"
         (sql/plan "SELECT foo.bar FROM foo FOR VALID_TIME ALL"
                   {:table-info {#xt/table foo #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-update"
         (sql/plan "UPDATE users FOR ALL VALID_TIME SET first_name = 'Sue'"
                   {:table-info {#xt/table users #{"first_name"}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-delete"
         (sql/plan "DELETE FROM users FOR ALL VALID_TIME"
                   {:table-info {#xt/table users #{}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-delete"
         (sql/plan "DELETE FROM users FOR VALID_TIME ALL"
                   {:table-info {#xt/table users #{}}}))))

(t/deftest test-for-all-system-time-404
  (t/is (=plan-file
         "test-for-all-system-time-404"
         (sql/plan "SELECT bar FROM foo FOR ALL SYSTEM_TIME"
                   {:table-info {#xt/table foo #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-system-time-404"
         (sql/plan "SELECT foo.bar FROM foo FOR SYSTEM_TIME ALL"
                   {:table-info {#xt/table foo #{"bar"}}}))))

(t/deftest test-period-specs-with-subqueries-407
  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-system-time"
    (sql/plan
     "SELECT 1 FROM (select foo.bar from foo FOR ALL SYSTEM_TIME) as tmp"
     {:table-info {#xt/table foo #{"bar"}}})))

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-app-time"
    (sql/plan
     "SELECT 1 FROM (select foo.bar from foo FOR VALID_TIME AS OF NOW) as tmp"
     {:table-info {#xt/table foo #{"bar"}}})))

  (t/is
   (=plan-file
    "test-period-specs-with-dml-subqueries-and-defaults-407" ;;also #424
    (sql/plan "INSERT INTO prop_owner (_id, customer_number, property_number, _valid_from, _valid_to)
                SELECT 1,
                145,
                7797, DATE '1998-01-03', tmp.app_start
                FROM
                (SELECT MIN(Prop_Owner._system_from) AS app_start
                FROM Prop_Owner
                FOR ALL SYSTEM_TIME
                WHERE Prop_Owner.id = 1) AS tmp"
              {:table-info {#xt/table prop_owner #{"id"}}}))))

(t/deftest parenthesized-joined-tables-are-unboxed-502
  (t/is (= (sql/plan "SELECT 1 FROM ( tab0 JOIN tab2 ON TRUE )"
                     {:table-info {#xt/table tab0 #{}, #xt/table tab2 #{}}})
           (sql/plan "SELECT 1 FROM tab0 JOIN tab2 ON TRUE"
                     {:table-info {#xt/table tab0 #{}, #xt/table tab2 #{}}}))))

(t/deftest test-delimited-identifiers-in-insert-column-list-2549
  (t/is (=plan-file
         "test-delimited-identifiers-in-insert-column-list-2549"
         (sql/plan
          "INSERT INTO posts (\"_id\", \"user-id\") VALUES (1234, 5678)")))

  (t/is (=plan-file
         "test-delimited-identifiers-in-insert-column-list-2549"
         (sql/plan
          "INSERT INTO posts RECORDS {_id: 1234, \"user-id\": 5678}"))))

(t/deftest test-table-period-specification-ordering-2260
  (let [opts {:table-info {#xt/table foo #{"bar"}}}
        v-s (sql/plan
             "SELECT foo.bar
               FROM foo
                 FOR ALL VALID_TIME
                 FOR ALL SYSTEM_TIME"
             opts)
        s-v (sql/plan
             "SELECT foo.bar
               FROM foo
                 FOR ALL SYSTEM_TIME
                 FOR ALL VALID_TIME"
             opts)]

    (t/is (=plan-file "test-table-period-specification-ordering-2260-v-s" v-s))

    (t/is (=plan-file "test-table-period-specification-ordering-2260-s-v" s-v))

    (t/is (= v-s s-v))))

(t/deftest array-agg-decorrelation

  (t/testing "ARRAY_AGG is not decorrelated using rule-9 as array_agg(null) =/= array_agg(empty-rel)"

    (t/is (=plan-file
           "array-agg-decorrelation-1"
           (sql/plan "SELECT (SELECT sum(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0"
                     {:table-info {#xt/table tab0 #{"z"}}})))
    (t/is (=plan-file
           "array-agg-decorrelation-2"
           (sql/plan "SELECT (SELECT ARRAY_AGG(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0"
                     {:table-info {#xt/table tab0 #{"z"}}})))))

(t/deftest test-order-by-3065
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3}]
                           [:put-docs :docs {:xt/id 2 :x 2}]
                           [:put-docs :docs {:xt/id 3 :x 1}]])

  (t/is (= [{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM docs ORDER BY docs.x")))

  (t/is (= [{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM docs ORDER BY docs.x + 1")))

  (t/is (= #{{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}}
           (set (xt/q tu/*node* "SELECT * FROM docs ORDER BY 1 + 1"))))

  (t/is (= [{:xt/id 3} {:xt/id 2} {:xt/id 1}]
           (xt/q tu/*node* "SELECT docs._id FROM docs ORDER BY docs.x"))
        "projected away order col")

  (t/is (= [{:xt/id 1} {:xt/id 2} {:xt/id 3}]
           (xt/q tu/*node* "SELECT docs._id FROM docs ORDER BY 1"))
        "order by column idx"))

(t/deftest test-order-by-unqualified-derived-column-refs
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3}]
                           [:put-docs :docs {:xt/id 2 :x 2}]
                           [:put-docs :docs {:xt/id 3 :x 1}]])

  (t/is (= [{:b 2} {:b 3} {:b 4}]
           (xt/q tu/*node* "SELECT (docs.x + 1) AS b FROM docs ORDER BY b")))
  (t/is (= [{:b 1} {:b 2} {:b 3}]
           (xt/q tu/*node* "SELECT docs.x AS b FROM docs ORDER BY b")))
  (t/is (= [{:x 1} {:x 2} {:x 3}]
           (xt/q tu/*node* "SELECT y.x FROM docs AS y ORDER BY x")))

  #_ ; TODO error - can't take an output and _then_ re-project (Postgres bans it, fwiw)
  (t/is (= [{:b 1} {:b 2} {:b 3}]
           (xt/q tu/*node* "SELECT docs.x AS b FROM docs ORDER BY (b + 2)"))))

(t/deftest test-select-star-projections
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3 :y "a"}]
                           [:put-docs :docs {:xt/id 2 :x 2 :y "b"}]
                           [:put-docs :docs {:xt/id 3 :x 1 :y "c"}]])

  (t/is (= [{:y "b"} {:y "a"} {:y "c"}]
           (xt/q tu/*node* "SELECT docs.y FROM docs")))

  (t/is (= #{{:x 2, :y "b", :bar "b", :xt/id 2}
             {:x 3, :y "a", :bar "a", :xt/id 1}
             {:x 1, :y "c", :bar "c", :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs.y AS bar FROM docs"))))

  (t/is (= #{{:x 2, :new-y "b", :xt/id 2}
             {:x 3, :new-y "a", :xt/id 1}
             {:x 1, :new-y "c", :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.* RENAME y AS new_y FROM docs"))))

  (t/is (= #{{:xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 2,
              :y "b",
              :xt/id 2}
             {:xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 3,
              :y "a",
              :xt/id 1}
             {:xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 1,
              :y "c",
              :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs._valid_from FROM docs"))))

  (t/is (= #{{:x 2,
              :y "b",
              :xt/id 2,
              :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 3,
              :y "a",
              :xt/id 1,
              :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 1,
              :y "c",
              :xt/id 3,
              :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}}
           (set (xt/q tu/*node* "SELECT *, _valid_from FROM docs WHERE _system_to = _valid_to OR (_system_to IS NULL AND _valid_to IS NULL)")))))

(t/deftest able-to-select-star-from-two-tables-bug-3389
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :x 1}]
                            [:put-docs :bar {:xt/id 2, :y 2}]])

  (t/is (= [{:foo-id 1, :x 1, :bar-id 2, :y 2}]
           (xt/q tu/*node* "SELECT foo.* RENAME _id AS foo_id,
                                   bar.* RENAME _id AS bar_id
                            FROM foo, bar"))))

(t/deftest test-select-star-asterisk-clause
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3 :y "a"}]
                           [:put-docs :docs {:xt/id 2 :x 2 :y "b"}]
                           [:put-docs :docs {:xt/id 3 :x 1 :y "c"}]])

  (t/is (= [{:xt/id 1, :x 3, :y "a"}]
           (xt/q tu/*node* "SELECT * FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1, :x 3, :y "a",
             :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"
             :xt/system-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _system_from, _system_to, _valid_from, _valid_to FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1, :y "a"}]
           (xt/q tu/*node* "SELECT * EXCLUDE x FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* "SELECT * EXCLUDE (x, y) FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1, :x 3, :z "a"}]
           (xt/q tu/*node* "SELECT * RENAME y AS z FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1, :y 3, :z "a"}]
           (xt/q tu/*node* "SELECT * RENAME (x AS y, y AS z) FROM docs WHERE _id = 1")))

  (t/is (= [{:xt/id 1, :y 3}]
           (xt/q tu/*node* "SELECT * EXCLUDE y RENAME (x AS y, y AS z) FROM docs WHERE _id = 1"))))

(t/deftest test-select-star-qualified-join
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]
                           [:put-docs :bar {:xt/id 2 :a "one"}]])

  #_ ; FIXME should throw
  (t/is (= [{:xt/id 1, :a "one", :xt/id:1 2}]
           (xt/q tu/*node* "SELECT * FROM foo JOIN bar ON true")))

  #_ ; FIXME should throw
  (t/is (= [{:xt/id 1, :a "one", :xt/id:1 2}]
           (xt/q tu/*node* "FROM foo JOIN bar ON true")))

  (t/is (= [{:bar-id 2, :a "one", :foo-id 1}]
           (xt/q tu/*node* "SELECT * RENAME (foo._id AS foo_id, bar._id AS bar_id) FROM foo JOIN bar ON true"))))

(t/deftest test-expand-asterisk-parenthesized-joined-table
  ;;Parens in the place of a table primary creates a parenthesized_joined_table
  ;;which is a qualified join, but nested inside a table primary. This test checks
  ;;that the expand asterisk attribute continues to traverse through the outer table
  ;;primary to find the nested table primaries.

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]
                           [:put-docs :bar {:xt/id 2 :a "one"}]])

  (t/is (= [{:foo-id 1 :bar-id 2 :a "one"}]

           (xt/q tu/*node*
                 "SELECT * RENAME (foo._id AS foo_id, bar._id AS bar_id) FROM ( foo LEFT JOIN bar ON true )"))))

(t/deftest test-select-star-lateral-join
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one"}]])

  (t/is (= [{:x-b "one", :outer-b "one", :z-b "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x (x_b), LATERAL (SELECT y.b AS z_b, x_b FROM y) AS z (z_b, outer_b)"))))

(t/deftest test-select-star-subquery
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one" :a 2}]])

  (t/is (= [{:b "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x"))))

(t/deftest test-sql-over-scanning
  (t/is
   (=plan-file
    "test-sql-over-scanning-col-ref"
    (sql/plan "SELECT foo.name FROM foo"
              {:table-info {#xt/table foo #{"name" "lastname"}}}))

   "Tests only those columns required by the query are scanned for, rather than all those present on the base table")

  (t/is
   (=plan-file
    "test-sql-over-scanning-qualified-asterisk"
    (sql/plan "SELECT foo.*, bar.jame FROM foo, bar"
              {:table-info {#xt/table foo #{"name" "lastname"}
                            #xt/table bar #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk"
    (sql/plan "SELECT * FROM foo, bar"
              {:table-info {#xt/table foo #{"name" "lastname"}
                            #xt/table bar #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-subquery"
    (sql/plan "SELECT foo.*, (SELECT * FROM baz) FROM foo, bar"
              {:table-info {#xt/table foo #{"name" "lastname"}
                            #xt/table bar #{"jame" "lastjame"}
                            #xt/table baz #{"frame"}}})))
  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-from-subquery"
    (sql/plan "SELECT bar.* FROM (SELECT foo.a, foo.b FROM foo) AS bar"
              {:table-info {#xt/table foo #{"a" "b"}}}))))

(t/deftest test-schema-qualified-names
  (t/is
   (=plan-file
    "test-schema-qualified-names-fully-qualified"
    (sql/plan "SELECT information_schema.columns.column_name FROM information_schema.columns")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-aliased-table"
    (sql/plan "SELECT f.column_name FROM information_schema.columns AS f")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-implict-pg_catalog"
    (sql/plan "SELECT pg_attribute.attname FROM pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-unqualified-col-ref"
    (sql/plan "SELECT pg_attribute.attname FROM pg_catalog.pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-qualified-col-ref"
    (sql/plan "SELECT pg_catalog.pg_attribute.attname FROM pg_attribute")))

  #_ ; FIXME field access
  (t/is
   (=plan-file
    "test-schema-qualified-names-field"
    (sql/plan "SELECT information_schema.columns.column_name.my_field FROM information_schema.columns")))

  ;;errors
  ;;
  #_ ; TODO these now return warnings which we should arguably test for
  (t/testing "Invalid Queries"
    (t/is
     (thrown-with-msg?
      Incorrect
      #"PG_CATALOG.columns.column_name is an invalid reference to columns, schema name does not match"
      (sql/plan "SELECT pg_catalog.columns.column_name FROM information_schema.columns")))

    (t/is
     (thrown-with-msg?
      Incorrect
      #"INFORMATION_SCHEMA.f.column_name is an invalid reference to f, schema name does not match"
      (sql/plan "SELECT information_schema.f.column_name FROM information_schema.columns AS f")))

    (t/is
     (thrown-with-msg?
      Incorrect
      #"PG_CATALOG.f.column_name is an invalid reference to f, schema name does not match"
      (sql/plan "SELECT pg_catalog.f.column_name FROM information_schema.columns AS f")))))

(t/deftest test-generated-column-names
  (t/is (= [{:xt/column-1 1, :xt/column-2 3}]
           (xt/q tu/*node* "SELECT LEAST(1,2), LEAST(3,4) FROM (VALUES (1)) x")))

  (t/testing "Aggregates"
    (t/is (= [{:xt/column-1 1}]
             (xt/q tu/*node* "SELECT COUNT(*) FROM (VALUES (1)) x"))))

  (t/testing "ARRAY()"
    (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id 1 :a 42}]
                             [:put-docs :b {:xt/id 2 :b1 "one" :b2 42}]])

    (t/is (= [{:xt/column-1 ["one"]}]
             (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42")))))

(t/deftest test-select-without-from
  (t/is (= [{:xt/column-1 1}]
           (xt/q tu/*node* "SELECT 1")))

  (t/is (= [{:xt/column-1 1 :xt/column-2 2}]
           (xt/q tu/*node* "SELECT 1, 2")))

  (t/is (= [{:xt/column-1 "xtdb"}]
           (xt/q tu/*node* "SELECT current_user"))))

(t/deftest test-nest
  (t/is (=plan-file "test-nest-one"
                    (sql/plan "SELECT _id AS order_id, value,
                            NEST_ONE(SELECT c.name FROM customers c WHERE c._id = o.customer_id) AS customer
                     FROM orders o"
                              {:table-info {#xt/table orders #{"_id" "value" "customer_id"}
                                            #xt/table customers #{"_id" "name"}}})))

  (t/is (=plan-file "test-nest-many"
                    (sql/plan "SELECT c._id AS customer_id, c.name,
                            NEST_MANY(SELECT o._id AS order_id, o.value
                                      FROM orders o
                                      WHERE o.customer_id = c._id)
                              AS orders
                     FROM customers c"
                              {:table-info {#xt/table orders #{"_id" "value" "customer_id"}
                                            #xt/table customers #{"_id" "name"}}})))

  (xt/submit-tx tu/*node* [[:put-docs :customers {:xt/id 0, :name "bob"}]
                           [:put-docs :customers {:xt/id 1, :name "alice"}]
                           [:put-docs :orders {:xt/id 0, :customer-id 0, :value 26.20}]
                           [:put-docs :orders {:xt/id 1, :customer-id 0, :value 8.99}]
                           [:put-docs :orders {:xt/id 2, :customer-id 1, :value 12.34}]])

  (t/is (= #{{:customer {:name "bob"}, :order-id 0, :value 26.20}
             {:customer {:name "bob"}, :order-id 1, :value 8.99}
             {:customer {:name "alice"}, :order-id 2, :value 12.34}}
           (set (xt/q tu/*node*
                      "SELECT o._id AS order_id, o.value,
                              NEST_ONE(SELECT c.name FROM customers c WHERE c._id = o.customer_id) AS customer
                       FROM orders o"))))

  (t/is (= #{{:orders [{:order-id 1, :value 8.99} {:order-id 0, :value 26.20}], :name "bob", :customer-id 0}
             {:orders [{:order-id 2, :value 12.34}], :name "alice", :customer-id 1}}
           (set (xt/q tu/*node*
                      "SELECT c._id AS customer_id, c.name,
                              NEST_MANY(SELECT o._id AS order_id, o.value
                                        FROM orders o
                                        WHERE o.customer_id = c._id)
                                AS orders
                       FROM customers c")))))

(t/deftest test-invalid-xt-id-in-query-3324
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0 :name "bob"}]])
  (t/is (= [] (xt/q tu/*node* "SELECT foo.name FROM foo WHERE foo._id = NULL")))
  (t/is (= [] (xt/q tu/*node* ["SELECT foo.name FROM foo WHERE foo._id = ?" nil]))))

#_ ; TODO Add this?
(t/deftest test-random-fn
  (t/is (= true (-> (xt/q tu/*node* "SELECT 0.0 <= random() AS greater") first :greater)))
  (t/is (= true (-> (xt/q tu/*node* "SELECT random() < 1.0 AS smaller ") first :smaller))))

(t/deftest test-tx-ops-sql-params
  (t/testing "correct number of args"
    (t/is (= (serde/->TxKey 0 #xt/instant "2020-01-01T00:00:00Z")
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users RECORDS {_id: ?, u_name: ?}" [1 "dan"] [2 "james"]]])))

    (t/is (= [{:u-name "dan", :xt/id 1}
              {:u-name "james", :xt/id 2}]
             (xt/q tu/*node* "SELECT users._id, users.u_name FROM users ORDER BY _id"))))

  (t/is (anomalous? [:incorrect :psql/invalid-parameter-value
                     "No value specified for parameter 1."
                     {:psql/state "22023",
                      :sql "INSERT INTO users(_id, u_name) VALUES (?, ?)",
                      :arg-rows nil}]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)"]]))
        "no arg rows provided when args expected")

  (t/is (anomalous? [:incorrect :psql/invalid-parameter-value
                     "No value specified for parameter 2."
                     {:psql/state "22023",
                      :sql "INSERT INTO users(_id, u_name) VALUES (?, ?)",
                      :arg-rows [[3] [4]]}]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)" [3] [4]]]))
        "incorrect number of args on all arg-row")

  #_ ; FIXME see https://github.com/seancorfield/next-jdbc/issues/303
  (t/testing "incorrect number of args on one row"
    (t/is (anomalous? [:incorrect nil
                       #"All SQL arg-rows must have the same number of columns"]
                      (xt/submit-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)" [3 "finn"] [4]]])))

    (t/is (= []
             (xt/q tu/*node* "SELECT users._id, users.u_name FROM users ORDER BY _id"))
          "no rows inserted due to error")))

(t/deftest test-case-sensitivity-in-delimited-cols
  (t/is (xt/execute-tx tu/*node* [[:sql "INSERT INTO T1(_id, col1, col2) VALUES(1,'fish',1000)"]]))
  (t/is (xt/execute-tx tu/*node* [[:sql "INSERT INTO t1(_id, COL1, COL2) VALUES(2,'dog',2000)"]]))

  (t/is (= [{:xt/id 2, :col1 "dog", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1._ID, T1.col1, t1.COL2 FROM T1")
           (xt/q tu/*node* "SELECT \"t1\"._id, T1.\"col1\", t1.COL2 FROM t1")))

  (t/is (xt/execute-tx tu/*node* [[:sql
                                   "INSERT INTO \"T1\"(_id, \"CoL1\", \"col2\") VALUES(3,'cat',3000)"]]))

  (t/is (= [{:xt/id 3, :CoL1 "cat", :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\"._id, \"T1\".\"CoL1\", \"T1\".COL2 FROM \"T1\"")))

  (t/is (= [{:xt/id 3, :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\"._id, \"T1\".col1, \"T1\".COL2 FROM \"T1\""))
        "can't refer to it as `col1`, have to quote")

  (t/is (xt/execute-tx tu/*node* [[:sql "UPDATE T1 SET Col1 = 'cat' WHERE t1.COL2 IN (313, 2000)"]]))

  (t/is (= [{:xt/id 2, :col1 "cat", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1._id, T1.col1, \"t1\".col2 FROM t1")))

  (t/is (= [{:col1 "cat", :avg 2000.0} {:col1 "fish", :avg 1000.0}]
           (xt/q tu/*node* "SELECT T1.col1, AVG(t1.col2) avg FROM t1 GROUP BY T1.col1")))

  (t/is (= [{:col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"TEEONE\".col2 FROM \"T1\" AS \"TEEONE\" WHERE \"TEEONE\".\"CoL1\" IN ( SELECT t1.\"col1\" FROM T1 WHERE T1.col1 = \"TEEONE\".\"CoL1\" ) ORDER BY \"TEEONE\".col2")))

  (t/is (xt/execute-tx tu/*node* [[:sql "DELETE FROM T1 WHERE t1.Col1 = 'fish'"]]))

  (t/is (= [{:xt/id 2}]
           (xt/q tu/*node* "SELECT t1._ID FROM T1")))

  (t/is (xt/execute-tx tu/*node* [[:sql
                                   "DELETE FROM \"T1\" WHERE \"T1\".\"col2\" IN (2000, 3000)"]]))

  (t/is (= [] (xt/q tu/*node* "SELECT \"T1\"._id FROM \"T1\""))))

(t/deftest test-ordering-of-intersect-and-union
  (xt/submit-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                           [:put-docs :t2 {:xt/id 1 :x 1}]
                           [:put-docs :t3 {:xt/id 1 :x 3}]])

  (t/is (= [{:x 1} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM t1 INTERSECT SELECT x FROM t2 UNION SELECT x FROM t3"))))

(t/deftest test-set-operations-with-different-column-names
  (t/testing "Union"
    (xt/execute-tx tu/*node* [[:put-docs :foo1 {:xt/id 1 :x 1}]
                              [:put-docs :bar1 {:xt/id 1 :y 2}]])

    (t/is (= [{:x 1} {:x 2}]
             (xt/q tu/*node* "SELECT x FROM foo1 UNION SELECT y FROM bar1"))))

  (t/testing "Except"
    (xt/execute-tx tu/*node* [[:put-docs :foo2 {:xt/id 1 :x 1}]
                              [:put-docs :foo2 {:xt/id 2 :x 2}]
                              [:put-docs :bar2 {:xt/id 1 :y 1}]])

    (t/is (= [{:x 2}]
             (xt/q tu/*node* "SELECT x FROM foo2 EXCEPT SELECT y FROM bar2"))))

  (t/testing "Intersect"
    (xt/execute-tx tu/*node* [[:put-docs :foo3 {:xt/id 1 :x 1}]
                              [:put-docs :foo3 {:xt/id 2 :x 2}]
                              [:put-docs :bar3 {:xt/id 1 :y 1}]])

    (t/is (= [{:x 1}]
             (xt/q tu/*node* "SELECT x FROM foo3 INTERSECT SELECT y FROM bar3")))))

(t/deftest test-union-all
  (xt/execute-tx tu/*node* [[:put-docs :foo1 {:xt/id 1 :x 1}]
                            [:put-docs :foo1 {:xt/id 2 :x 2}]
                            [:put-docs :foo2 {:xt/id 1 :x 1}]])

  (t/is (= [{:x 2} {:x 1}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION SELECT x FROM foo2")))

  (t/is (= [{:x 2} {:x 1} {:x 1}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION ALL SELECT x FROM foo2"))))

(t/deftest test-union-except
  (xt/execute-tx tu/*node* [[:put-docs :foo1 {:xt/id 1 :x 1}]
                            [:put-docs :foo1 {:xt/id 2 :x 2}]
                            [:put-docs :foo2 {:xt/id 1 :x 1}]
                            [:put-docs :foo3 {:xt/id 1 :x 3}]])

  (t/is (= [{:x 2} {:x 1} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION SELECT x FROM foo3")))

  (t/is (= [{:x 2}]
           (xt/q tu/*node* "SELECT x FROM foo1 EXCEPT SELECT x FROM foo2")))

  (t/is (= [{:x 2} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION SELECT x FROM foo3 EXCEPT SELECT x FROM foo2")))

  (t/is (= [{:x 2} {:x 3}]
           (xt/q tu/*node* "(SELECT x FROM foo1 EXCEPT SELECT x FROM foo2) UNION SELECT x FROM foo3")))

  (t/is (= [{:x 2} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM foo1 EXCEPT SELECT x FROM foo2 UNION SELECT x FROM foo3"))))

(t/deftest test-join-cond-subqueries
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, x: 1}, {_id: 2, x: 2}"]
                            [:sql "INSERT INTO bar (_id, x) VALUES (1, 1), (2, 3)"]
                            [:sql "INSERT INTO baz (_id, x) VALUES (1, 2)"]])
  (t/is (= #{{:x 2, :bar-x 3} {:x 2, :bar-x 1} {:x 1}}
           (-> (xt/q tu/*node* "SELECT foo.x, bar.x bar_x FROM foo LEFT JOIN bar ON foo.x = (SELECT baz.x FROM baz)")
               set)))

  (t/is (= #{{:x 2}, {:x 1}}
           (-> (xt/q tu/*node* "SELECT foo.x, bar.x bar_x FROM foo LEFT JOIN bar ON bar.x = (SELECT baz.x FROM baz WHERE baz.x = foo.x)")
               set))))

(t/deftest test-erase-with-subquery
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :foo :bar 1}]])
  (t/is (xt/execute-tx tu/*node* [[:sql "ERASE FROM docs WHERE docs._id IN (SELECT docs._id FROM docs WHERE docs.bar = 1)"]]))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest select-star-on-non-existent-tables-3005
  (t/is (= [] (xt/q tu/*node* "select users.first_name from users")))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM users"))))

(t/deftest nested-parenthesized-joined-tables-3185
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1 :x 1}]
                            [:put-docs :bar {:xt/id 1 :y 1}]
                            [:put-docs :baz {:xt/id 1 :z 1}]])

  (t/is (= [{:x 1, :y 1}]
           (xt/q tu/*node* "SELECT * EXCLUDE _id FROM ( foo LEFT JOIN bar ON true )")))

  (t/is (= [{:x 1, :y 1, :z 1}]
           (xt/q tu/*node* "SELECT * EXCLUDE _id FROM ( foo JOIN (bar JOIN baz ON true) ON true )"))))

(t/deftest test-select-same-col-twice-3025
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

  (t/is (= [{:something 1 :other 1}]
           (xt/q tu/*node* '(from :docs [{:xt/id something} {:xt/id other}]))))

  (t/is (= [{:something 1 :other 1}]
           (xt/q tu/*node* "SELECT _id as something, _id as other FROM docs"))))

(t/deftest test-delete-with-unqualified-col-3017
  (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id 1 :col "val"}]])

  (t/is (= [{:xt/id 1, :col "val"}]
           (xt/q tu/*node* "SELECT * FROM table")))

  (xt/submit-tx tu/*node* [[:sql "DELETE FROM table WHERE col = 'val'"]])

  (t/is (empty? (xt/q tu/*node* "SELECT * FROM table"))))

(t/deftest test-c-style-escapes-3415
  (t/is (= ["A" "A" "A" "" "%" "😁" "\n" "'"]
           (->> (xt/q tu/*node* "VALUES E'\\x41', E'\\101', E'\\U00000041', E'', E'%', E'\\U0001F601', E'\\n', E'\\''")
                (mapv :xt/column-1)))))

(t/deftest forbid-updating-core-cols-3433
  (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id 1}]])

  (letfn [(f [sql]
            (xt/execute-tx tu/*node* [[:sql sql]]))]

    (t/is (anomalous? [:incorrect nil
                       #"Cannot UPDATE _id column"]
                      (f "UPDATE table SET _id = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (anomalous? [:incorrect nil
                       #"Cannot UPDATE _valid_from column"]
                      (f "UPDATE table SET _valid_from = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (anomalous? [:incorrect nil
                       #"Cannot UPDATE _valid_to column"]
                      (f "UPDATE table SET _valid_to = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (anomalous? [:incorrect nil
                       #"Cannot UPDATE _system_from column"]
                      (f "UPDATE table SET _system_from = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (anomalous? [:incorrect nil
                       #"Cannot UPDATE _system_to column"]
                      (f "UPDATE table SET _system_to = DATE '2024-01-01' WHERE _id = 1")))))

(t/deftest test-assert-3445
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 1}]])

  (t/is (xt/execute-tx tu/*node* [[:sql "ASSERT (SELECT COUNT(*) FROM docs) = 1"]
                                  [:put-docs :docs {:xt/id 2 :x 2}]]))

  (t/is (anomalous? [:conflict nil #"Assert failed"]
                    (xt/execute-tx tu/*node* [[:sql "ASSERT (SELECT COUNT(*) FROM docs) = 1"]
                                              [:put-docs :docs {:xt/id 3 :x 3}]])))

  (t/is (= [{:doc-count 2}] (xt/q tu/*node* "SELECT COUNT(*) doc_count FROM docs"))))

(t/deftest test-assert-exists-3686-3689
  (t/is (xt/execute-tx tu/*node* [[:sql "ASSERT NOT EXISTS (SELECT 1 FROM users WHERE email = 'james@example.com')"]
                                  [:sql "INSERT INTO users RECORDS {_id: 'james', name: 'James', email: 'james@example.com'}"]]))

  (t/is (= [{:xt/id "james", :name "James", :email "james@example.com"}]
           (xt/q tu/*node* "SELECT * FROM users")))

  (t/is (anomalous? [:conflict nil #"Assert failed"]
                    (xt/execute-tx tu/*node* [[:sql "ASSERT NOT EXISTS (SELECT 1 FROM users WHERE email = 'james@example.com')"]
                                              [:sql "INSERT INTO users RECORDS {_id: 'james2', name: 'James 2', email: 'james@example.com'}"]])))

  (t/is (= [{:xt/id "james", :name "James", :email "james@example.com"}]
           (xt/q tu/*node* "SELECT * FROM users"))))

(t/deftest test-date-id-caught-3446
  (t/is (anomalous? [:incorrect nil #"Invalid ID type: java.time.LocalDate"]
                    (xt/execute-tx tu/*node* [[:sql "INSERT into readings (_id, value) VALUES (DATE '2020-01-06', 4)"]])))

  (t/testing "node continues"
    (xt/execute-tx tu/*node* [[:put-docs :readings {:xt/id 1 :value 4}]])

    (t/is (= [{:xt/id 1, :value 4}]
             (xt/q tu/*node* "SELECT * FROM readings")))))

(t/deftest test-disallow-col-refs-in-period-specs-3447
  (t/is (anomalous? [:incorrect nil
                     #"No column reference allowed in table period specification: foo"]
                    (xt/q tu/*node* "SELECT * FROM docs FOR SYSTEM_TIME AS OF foo")))

  (t/is (anomalous? [:incorrect nil
                     #"No column reference allowed in table period specification: bar"]
                    (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME BETWEEN bar AND baz"))))

(t/deftest test-portion-of-valid-time-boundary
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO system_power (_id, value, _valid_from, _valid_to)
VALUES
(1, 500,  TIMESTAMP '2024-01-01T00:00:00Z', TIMESTAMP '2024-01-01T00:05:00Z'),
(1, 510,  TIMESTAMP '2024-01-01T00:05:00Z', TIMESTAMP '2024-01-01T00:10:00Z'),
(1, 530,  TIMESTAMP '2024-01-01T00:10:00Z', TIMESTAMP '2024-01-01T00:15:00Z'),
(1, 9999, TIMESTAMP '2024-01-01T00:15:00Z', TIMESTAMP '2024-01-01T00:20:00Z'),
(1, 560,  TIMESTAMP '2024-01-01T00:20:00Z', TIMESTAMP '2024-01-01T00:25:00Z'),
(1, 580,  TIMESTAMP '2024-01-01T00:25:00Z', TIMESTAMP '2024-01-01T00:30:00Z')"]])

  (letfn [(zdt [mins]
            (.plusMinutes #xt/zoned-date-time "2024-01-01T00:00Z[UTC]" mins))
          (q []
            (set (xt/q tu/*node* "SELECT value, _valid_from, _valid_to
                                  FROM system_power FOR ALL VALID_TIME WHERE _id = 1")))
          (del! [from to]
            (xt/execute-tx tu/*node* [[:sql "DELETE FROM system_power
                                             FOR PORTION OF VALID_TIME FROM ? TO ?
                                             WHERE _id = 1"
                                       [from to]]]))]

    (t/is (del! (zdt 15) (zdt 20)))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 5)}
               {:value 510, :xt/valid-from (zdt 5), :xt/valid-to (zdt 10)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 560, :xt/valid-from (zdt 20), :xt/valid-to (zdt 25)}
               {:value 580, :xt/valid-from (zdt 25), :xt/valid-to (zdt 30)}}
             (q)))

    (t/is (del! (zdt 2) (zdt 10)))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 2)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 560, :xt/valid-from (zdt 20), :xt/valid-to (zdt 25)}
               {:value 580, :xt/valid-from (zdt 25), :xt/valid-to (zdt 30)}}
             (q)))

    (t/is (del! (zdt 20) (zdt 27)))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 2)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 580, :xt/valid-from (zdt 27), :xt/valid-to (zdt 30)}}
             (q)))))

(t/deftest contains-precedence-bug-3473
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs1 RECORDS {_id: 1}"]
                           [:sql "INSERT INTO docs2 RECORDS {_id: 1}"]])

  (t/is (= [{:one 1}] (xt/q tu/*node* "
SELECT 1 AS one
FROM docs1 FOR VALID_TIME ALL AS d1
JOIN docs2 FOR VALID_TIME ALL AS d2
    ON d1._VALID_TIME CONTAINS d2._valid_from AND d1._id = d2._id")))

  (t/is (= [{:one 1}] (xt/q tu/*node* "
SELECT 1 AS one
FROM docs1 FOR VALID_TIME ALL AS d1
JOIN docs2 FOR VALID_TIME ALL AS d2
    ON d1._id = d2._id AND d1._VALID_TIME CONTAINS d2._valid_from"))))

(t/deftest bound-period-contains-timestamp-expression-4381
  (t/is (= [{:t true}] (xt/q tu/*node* "
SELECT PERIOD(DATE '2022-12-31', TIMESTAMP '2023-01-02') CONTAINS (DATE '2023-01-01' + INTERVAL 'PT0S') AS t"))))

(t/deftest unescapes-escaped-quotes-3467
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (' ''foo'' ')"]])
  (t/is (= [{:xt/id " 'foo' "}] (xt/q tu/*node* "SELECT * FROM foo"))))

(t/deftest info-schema-case-insensitivity-3511
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])
  (t/is (= '[_id _system_from _system_to _valid_from _valid_to]
           (->> (xt/q tu/*node* "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'foo' ORDER BY column_name")
                (mapv (comp symbol :column-name)))
           (->> (xt/q tu/*node* "SELECT column_name FROM information_schema.columns WHERE table_name = 'foo' ORDER BY column_name")
                (mapv (comp symbol :column-name))))))

(t/deftest test-sql->static-ops-3484
  (let [opts {:table-info {#xt/table foo #{"bar"}}}]
    (t/testing "non-inserts"
      (t/is (nil? (sql/sql->static-ops "UPDATE foo SET bar = baz" nil opts)))
      (t/is (nil? (sql/sql->static-ops "DELETE FROM foo WHERE bar = 1" nil opts)))
      (t/is (nil? (sql/sql->static-ops "ERASE FROM foo WHERE bar = 1" nil opts))))

    (t/is (nil? (sql/sql->static-ops "INSERT INTO baz (bar) SELECT bar FROM foo" nil))
          "excludes insert-from-subquery"))

  (t/is (= [(tx-ops/map->PutDocs {:table-name 'public/foo, :docs [{"_id" 1, "v" 2}]})]
           (sql/sql->static-ops "INSERT INTO foo (_id, v) VALUES (1, 2)" nil)))

  (t/is (nil? (sql/sql->static-ops "INSERT INTO foo (_id, v) VALUES (1, 2 + 3)" nil))
        "excludes expressions")

  (t/is (= [(tx-ops/map->PutDocs {:table-name 'public/foo, :docs [{"_id" 1} {"_id" 2}]
                                  :valid-from #xt/date "2020-08-01"})
            (tx-ops/map->PutDocs {:table-name 'public/foo, :docs [{"_id" 3}]
                                  :valid-from #xt/date "2021-01-01"})]

           (sql/sql->static-ops "INSERT INTO foo (_id, _valid_from) VALUES (1, DATE '2020-08-01'), (2, DATE '2020-08-01'), (3, DATE '2021-01-01')" nil))
        "groups by valid-from")

  (t/testing "with args"
    (t/is (= [(tx-ops/map->PutDocs {:table-name 'public/foo, :docs [{"_id" 1} {"_id" 3}]
                                    :valid-from #xt/date "2020-01-01"})
              (tx-ops/map->PutDocs {:table-name 'public/foo, :docs [{"_id" 2} {"_id" 4}]
                                    :valid-from #xt/date "2020-01-02"})]

             (sql/sql->static-ops "INSERT INTO foo (_id, _valid_from) VALUES (?, DATE '2020-01-01'), (?, DATE '2020-01-02')"
                                  [[1 2] [3 4]]))))

  (t/testing "insert records"
    (t/is (= [(tx-ops/map->PutDocs {:table-name 'public/bar, :docs [{"_id" 0, "value" "hola"} {"_id" 1, "value" "mundo"}],
                                    :valid-from nil, :valid-to nil})]
             (sql/sql->static-ops "INSERT INTO bar RECORDS $1"
                                  [[{"_id" 0, "value" "hola"}]
                                   [{"_id" 1, "value" "mundo"}]])))))

(t/deftest test-sql->static-ops-decimals-4483
  (t/is (= [(tx-ops/map->PutDocs {:table-name 'public/foo,
                                  :docs [{"xt/id" 1, "dec" 1.01M} {"xt/id" 2, "dec" 1.012M}],
                                  :valid-from nil, :valid-to nil})]
           (sql/sql->static-ops "INSERT INTO foo RECORDS ?"
                                [[{:xt/id 1, :dec 1.01M}]
                                 [{:xt/id 2, :dec 1.012M}]]))))

(t/deftest show-canned-responses
  (t/is (= [{:transaction-isolation "read committed"}]
           (xt/q tu/*node* "SHOW TRANSACTION ISOLATION LEVEL")))

  (t/is (= [{:version "PostgreSQL 16"}]
           (xt/q tu/*node* "SELECT pg_catalog.version()")))

  (t/is (= [{:timezone "America/New_York"}]
           (xt/q tu/*node* "SHOW TIME ZONE"
                 {:default-tz #xt/zone "America/New_York"}))))

(t/deftest test-regclass
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/testing "regclass uses search path to resolve unqualified names"
    (t/is (= [{:v (RegClass. 357712798)}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass v")
             (xt/q tu/*node* "SELECT 'foo'::regclass v"))
          "text -> regclass"))

  (t/testing "regclass name is returned qualified only if the schema does not appear in search path"
    (t/is (= [{:v "foo"}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass::varchar v"))
          "text -> regclass -> text")

    (t/is (= [{:v "information_schema.columns"}]
             (xt/q tu/*node* "SELECT 'information_schema.columns'::regclass::varchar v"))
          "text -> regclass -> text"))

  (t/is (= [{:v 357712798}]
           (xt/q tu/*node* "SELECT 'public.foo'::regclass::int v"))
        "text -> regclass -> int")

  (t/is (= [{:v (RegClass. 1151209360)}]
           (xt/q tu/*node* "SELECT 1151209360::regclass v"))
        "int -> regclass")

  (t/is (anomalous? [:incorrect nil
                     #"Relation public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regclass")))

  (t/testing "regclass that doesn't match a known table"
    (t/is (= [{:v "999999"}]
             (xt/q tu/*node* "SELECT 999999::regclass::varchar v"))
          "returns a stringified oid/int"))

  (t/is (= '[_id _system_from _system_to _valid_from _valid_to]
           (->> (xt/q tu/*node* "SELECT attname FROM pg_attribute WHERE attrelid = 'public.foo'::regclass ORDER BY attname")
                (mapv (comp symbol :attname)))))

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 357712798::regclass = 'foo'::regclass v"))
        "regclass with identical oid are equal")

  (t/testing "testing non-literal cast"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1, tn: 'foo'}"]])

    (t/is (= [{:v (RegClass. 357712798)}]
             (xt/q tu/*node* "SELECT tn::regclass v FROM bar"))
          "text -> regclass")))

(t/deftest test-regclass-where-expr
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1}"]
                           [:sql "INSERT INTO bar RECORDS {_id: 2}"]])

  (t/is (= [{:xt/id 2} {:xt/id 1}]
           (xt/q tu/*node* "SELECT _id FROM bar WHERE 'bar'::regclass = 904292726"))))

(t/deftest test-regproc
  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 'pg_catalog.array_in'::regproc v")
           (xt/q tu/*node* "SELECT 'array_in'::regproc v"))
        "text -> regproc")

  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 1989914641::regproc v"))
        "int -> regproc")

  (t/is (anomalous? [:incorrect nil
                     #"Procedure public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regproc")))

  (t/is (= [{:v "999999"}]
           (xt/q tu/*node* "SELECT 999999::regproc::varchar v"))
        "returns a stringified oid/int")

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 1989914641::regproc = 'array_in'::regproc v"))
        "regproc with identical oid are equal"))

(t/deftest test-regclass-search-path-precedence
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO pg_class RECORDS {_id: 1}"]])

  (t/is (= [{:v (RegClass. 529124840)}]
           (xt/q tu/*node* "SELECT 'pg_class'::regclass v")
           (xt/q tu/*node* "SELECT 'pg_catalog.pg_class'::regclass v"))
        "matches pg_catalog.pg_class over public.pg_class as the former comes first in the search path"))

(t/deftest valid-time-as-col-name-3661
  (t/is (= [{:xt/valid-time 1, :xt/system-time 2}]
           (xt/q tu/*node* "SELECT 1 AS _valid_time, 2 AS _system_time"))))

(t/deftest test-xt-version
  (t/is (str/starts-with? (:version (first (xt/q tu/*node* "SELECT xt.version()")))
                          "XTDB @")))

(t/deftest records-query
  (t/is (=plan-file "records-query"
                    (sql/plan "RECORDS {_id: 1, x: 1}, {x: 2.0}")))

  (t/is (= [{:x 1, :xt/id 1} {:x 2.0}]
           (xt/q tu/*node* "RECORDS {_id: 1, x: 1}, {x: 2.0}"))))

(t/deftest insert-record-literals
  (t/is (=plan-file "insert-record-literals"
                    (sql/plan "INSERT INTO foo RECORDS {_id: 1, x: 2}")))

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, x: 2}"]])

  (t/is (anomalous? [:incorrect :missing-id]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {id: 1, x: 2}"]])))

  (t/is (= [{:x 2, :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM foo")))

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO bar RECORDS ?"
                             [{:_id 2, :x 3}]
                             [{:_id 3, :x 4.0}]]

                            [:sql "INSERT INTO bar RECORDS ?, {_id: 4, x: 5}"
                             [{:_id 5, :x 6.0}]]

                            [:sql "INSERT INTO bar RECORDS ?"
                             [{:_id 7, :x "8"}]]])

  (t/is (= [{:xt/id 2, :x 3} {:xt/id 3, :x 4.0} {:xt/id 4, :x 5} {:xt/id 5, :x 6.0} {:xt/id 7, :x "8"}]
           (xt/q tu/*node* "SELECT * FROM bar ORDER BY _id")))

  (t/is (anomalous? [:incorrect :missing-id]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo RECORDS ?"
                                               [{:id 2, :x 3}]]]))))

(t/deftest limit-parens-3475
  (let [q "
(SELECT _id, foo FROM bar LIMIT 1)
UNION ALL
(SELECT _id, foo FROM baz LIMIT 1)
"]
    (t/is (=plan-file "limit-parens"
                      (sql/plan q
                                {:table-info {#xt/table bar #{"_id" "foo"}
                                              #xt/table baz #{"_id" "foo"}}})))

    (xt/submit-tx tu/*node*
                  [[:put-docs :bar {:xt/id 1 :foo 2} {:xt/id 2 :foo 3}]
                   [:put-docs :baz {:xt/id 3 :foo 4} {:xt/id 4 :foo 5}]])

    (t/is (= [{:xt/id 2, :foo 3} {:xt/id 4, :foo 5}]
             (xt/q tu/*node* q)))))

(t/deftest test-generate-series-3212
  (t/is (= [1 2 3]
           (->> (xt/q tu/*node* "SELECT x FROM generate_series(1, 4) xs (x)")
                (mapv :x))))

  (t/is (= [1 4 7]
           (->> (xt/q tu/*node* "SELECT x FROM generate_series(1, 8, 3) xs (x)")
                (mapv :x))))

  (t/is (empty? (xt/q tu/*node* "SELECT x FROM generate_series(10, 3) xs (x)")))

  (t/is (empty? (-> (xt/q tu/*node* "SELECT x FROM generate_series(1, 1) xs (x)"))))

  (t/is (= [1]
           (->> (xt/q tu/*node* "SELECT x FROM generate_series(1, 2, 2) xs (x)")
                (mapv :x))))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, start: -1, end: 3}, {_id: 2, start: 4, end: 6}"]])

  (t/is (= [{:xt/id 2, :x 4}
            {:xt/id 2, :x 5}
            {:xt/id 1, :x -1}
            {:xt/id 1, :x 0}
            {:xt/id 1, :x 1}
            {:xt/id 1, :x 2}]
           (-> (xt/q tu/*node* "SELECT _id, x FROM foo, generate_series(start, end) xs (x)")))))

(t/deftest star-goes-at-end-too-3706
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, x: 'foo'}"]])

  (t/is (= '#{[xtdb public/foo _id :i64]
              [xtdb public/foo _system_from :instant]
              [xtdb public/foo _system_to [:? :instant]]
              [xtdb public/foo _valid_from :instant]
              [xtdb public/foo _valid_to [:? :instant]]
              [xtdb public/foo x :utf8]}

           (->> (jdbc/execute! tu/*node*
                               ["SELECT column_name, * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'foo' ORDER BY column_name;"]
                               {:builder-fn xt-jdbc/builder-fn})
                (into #{} (map (juxt (comp symbol :table-catalog)
                                     (fn [{:keys [table-schema table-name]}]
                                       (symbol table-schema table-name))
                                     (comp symbol :column-name)
                                     (comp read-string :data-type))))))))

(t/deftest missing-values-in-insert-shouldnt-stop-ingestion-3721
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs (_id, foo) SELECT 3 AS _id"]])

  (t/is (= [] (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest test-insert-into-schema-qualified-table
  (t/is (=plan-file "insert-into-schema-qualified-table"
                    (sql/plan "INSERT INTO foo.bar RECORDS {_id: 1, x: 2}")))

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo.bar RECORDS {_id: 1, x: 2}"]])

  (t/is (=plan-file "select-from-schema-qualified-table"
                    (sql/plan "SELECT * FROM foo.bar"
                              {:table-info {#xt/table foo/bar #{"_id" "x"}}})))

  (t/is (= [{:x 2, :xt/id 1}] (xt/q tu/*node* "SELECT * FROM foo.bar")))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM bar")))

  (xt/execute-tx tu/*node* [[:sql "UPDATE foo.bar SET x = 3 WHERE _id = 1"]])
  (xt/execute-tx tu/*node* [[:sql "UPDATE bar SET x = x + 1 WHERE _id = 1"]])

  (t/is (= [{:x 3, :xt/id 1}] (xt/q tu/*node* "SELECT * FROM foo.bar")))

  (xt/execute-tx tu/*node* [[:sql "DELETE FROM bar WHERE _id = 1"]])

  (t/is (= [{:x 3, :xt/id 1}] (xt/q tu/*node* "SELECT * FROM foo.bar")))

  (xt/execute-tx tu/*node* [[:sql "DELETE FROM foo.bar WHERE _id = 1"]])
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo.bar")))
  (t/is (= [{:x 3, :xt/id 1} {:x 2, :xt/id 1}] (xt/q tu/*node* "SELECT * FROM foo.bar FOR ALL VALID_TIME"))))

(t/deftest ignore-returning-keys-3668
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs (_id, foo) VALUES(1, 'bar') RETURNING * EXCLUDE foo"]
                           [:sql "INSERT INTO docs (_id, foo) VALUES(2, 'bar'), (3, 'wuf') RETURNING _id"]
                           [:sql "UPDATE docs SET foo = 'toto' WHERE _id = 1 RETURNING _id + 1, foo AS bar"]
                           [:sql "DELETE FROM docs WHERE _id = 3 RETURNING *"]])

  (t/is (= [{:xt/id 2, :foo "bar"} {:xt/id 1, :foo "toto"}]
           (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest test-no-column-name-list-insert-with-values-3752
  (let [ex (t/is (anomalous? [:incorrect nil]
                             (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs VALUES(1, 'bar')"]])))
        ex-msg (ex-message ex)]
    (t/is (str/includes? ex-msg "INSERT with VALUES needs column name list"))
    (t/is (str/includes? ex-msg "INSERT does not contain mandatory _id column")))

  (t/is (= [] (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest read-write-promotion-conflict-in-live-rel-3709
  (t/testing "original repro"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs (_id, foo) VALUES (1, 'bar')"]])

    (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs SELECT * EXCLUDE _id, 3 AS _id FROM docs WHERE _id = 1"]])

    (t/is (= [{:xt/id 1,
               :foo "bar",
               :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}
              {:xt/id 3,
               :foo "bar",
               :xt/valid-from #xt/zoned-date-time "2020-01-02T00:00Z[UTC]"}]

             (xt/q tu/*node* "SELECT _id, foo, _valid_from FROM docs FOR ALL VALID_TIME"))))

  (t/testing "update repro"
    (xt/submit-tx tu/*node* [[:put-docs :docs2 {:xt/id 1, :foo "bar", :bar 1}]])
    (xt/submit-tx tu/*node* [[:sql "UPDATE docs2 SET bar = foo, foo = bar"]])

    (t/is (= [{:xt/id 1, :bar "bar", :foo 1,
               :xt/valid-from #xt/zoned-date-time "2020-01-04T00:00Z[UTC]"}
              {:xt/id 1, :bar 1, :foo "bar",
               :xt/valid-from #xt/zoned-date-time "2020-01-03T00:00Z[UTC]"}]
             (xt/q tu/*node* "SELECT *, _valid_from FROM docs2 FOR ALL VALID_TIME")))))

(t/deftest insert-with-bad-select-shouldnt-stop-ingestion-3797
  (xt/submit-tx tu/*node* ["INSERT INTO docs (_id, foo) SELECT 1"])
  (t/is (= 1 (count (xt/q tu/*node* '(from :xt/txs [error])))))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM docs"))))

(t/deftest test-boolean-cast
  (t/is (= [{:v true}] (xt/q tu/*node* ["SELECT ?::boolean v" 1]))))

(t/deftest disallow-slashes-in-delimited-identifiers-3799
  (t/is (anomalous? [:incorrect nil
                     #"token recognition error at: '\"zip/'"]
                    (xt/execute-tx tu/*node* ["INSERT INTO address (_id, \"zip/code\") VALUES (1, 123)"]))))

(t/deftest disallow-inserting-to-system-time-cols-3748
  (t/is (anomalous? [:incorrect nil #"Cannot put documents with columns: #\{\"_system_from\" \"_valid_time\"\}"]
                    (xt/execute-tx tu/*node* ["INSERT INTO docs (_id, _system_from, _valid_time) VALUES (1, TIMESTAMP '2024-01-01T00:00:00Z', 'foo')"])))

  (let [ex (t/is (anomalous? [:incorrect nil]
                             (xt/execute-tx tu/*node* ["INSERT INTO docs (_id, _system_from, _valid_time) VALUES (1, CURRENT_TIME - INTERVAL 'P1D', 'foo')"])))
        ex-msg (ex-message ex)]
    (t/is (str/includes? ex-msg "Cannot INSERT _valid_time column"))
    (t/is (str/includes? ex-msg "Cannot INSERT _system_from column")))

  (t/is (anomalous? [:incorrect nil #"Cannot put documents with columns: #\{\"_system_from\"\}"]
                    (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 1, _system_from: TIMESTAMP '2024-01-01T00:00:00Z'}"]))))

(t/deftest can-not-write-to-reserved-tables
  (t/testing "submit side"
    (t/is (anomalous? [:incorrect nil #"Cannot write to table: xt.txs"]
                      (xt/submit-tx tu/*node* [[:put-docs :xt/txs {:xt/id 1}]])))

    (t/is (anomalous? [:incorrect nil #"Cannot write to table: xt.txs"]
                      (xt/execute-tx tu/*node* ["INSERT INTO xt.txs(_id, system_time, committed, error) VALUES(1, 2, 3, 4)"])))

    (t/is (anomalous? [:incorrect nil #"Cannot write to table: pg_catalog.pg_user"]
                      (xt/execute-tx tu/*node* ["INSERT INTO pg_catalog.pg_user(_id, system_time, committed, error) VALUES(1, 2, 3, 4)"]))))

  ;; just to have something in tx.txs before the check below
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

  (t/testing "indexing side"
    (t/is (anomalous? [:incorrect nil
                       #"Cannot write to table: xt/txs"]
                      (xt/execute-tx tu/*node* ["INSERT INTO xt.txs (_id, system_time, committed, error) SELECT 1, 2, 3, 4"])))

    (xt/submit-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, field: 'foo'}"])

    ;; was stopping ingestion, trying to delete from an info-schema table
    (t/is (anomalous? [:incorrect nil
                       #"Cannot write to table: information_schema/columns"]
                      (xt/execute-tx tu/*node* ["DELETE FROM information_schema.columns WHERE column_name = 'field'"])))))

(t/deftest test-keyword
  (t/is (= [{:unq :foo, :q :foo/bar}]
           (xt/q tu/*node* "SELECT KEYWORD 'foo' AS unq, KEYWORD 'foo/bar' AS q")))

  (t/is (= [{:unq :foo, :q :foo/bar}]
           (xt/q tu/*node* "SELECT 'foo'::keyword AS unq, 'foo/bar'::keyword AS q")))

  (t/is (= [{:ns "foo", :local-name "bar"}, {:local-name "bar"}]
           (xt/q tu/*node* "SELECT NAMESPACE(kw) ns, LOCAL_NAME(kw) local_name FROM (VALUES (KEYWORD 'foo/bar'), (KEYWORD 'bar')) kw (kw)")))

  (t/is (= [{:k1 :foo/bar, :k2 :bar, :s1 ":foo/bar", :s2 ":bar"}]
           (xt/q tu/*node* "SELECT ':foo/bar'::keyword k1, 'bar'::keyword k2, (KEYWORD 'foo/bar')::text s1, (KEYWORD ':bar')::text s2"))))

(t/deftest test-char-type
  (t/testing "::char casts to utf8 (same as ::text and ::varchar)"
    (t/is (= [{:c "f"}]
             (xt/q tu/*node* "SELECT 'f'::char AS c")))

    (t/is (= [{:c "f", :t "f", :v "f"}]
             (xt/q tu/*node* "SELECT 'f'::char AS c, 'f'::text AS t, 'f'::varchar AS v"))))

  (t/testing "char(n) with length parameter is not supported - XTDB uses Arrow's variable-length utf8 for all strings"
    (t/is (anomalous? [:incorrect nil #"mismatched input '\('"]
                      (xt/q tu/*node* "SELECT 'foo'::char(10) AS c")))))

(t/deftest test-str
  (t/is (= [{:str "hello, 42.0 at 2020-01-01"}]
           (xt/q tu/*node* "SELECT STR('hello, ', NULL, 42.0, ' at ', DATE '2020-01-01') str"))))

(t/deftest insert-with-transit-param-3907
  (letfn [(jdbc-insert-txn [table records]
            (let [total (count records)
                  into-table (partial str "INSERT INTO " table " RECORDS ")
                  args (interpose ", " (repeat total "?"))]
              (into [(apply into-table args)] records)))

          (jdbc-insert-records [conn table records]
            (jdbc/execute! conn (jdbc-insert-txn table records) {:builder-fn xt-jdbc/builder-fn}))]

    (jdbc-insert-records tu/*node* "applications"
                         [{:_id "payee1"
                           :application {}}
                          {:_id "payee2"
                           :application {}}])

    (t/is (= {:app_count 2}
             (jdbc/execute-one! tu/*node* ["SELECT COUNT(*) app_count FROM applications"])))))

(t/deftest test-order-nulls-first-last
  (xt/execute-tx tu/*node* ["INSERT INTO foo (_id, name) VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')"])
  (xt/execute-tx tu/*node* ["INSERT INTO foo (_id, name) VALUES (1, 'fooz'), (2, 'barz'), (3, 'bazz')"])
  (t/is (=
         [nil nil nil
          #xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"
          #xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"
          #xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"]
         (->> (xt/q tu/*node* "SELECT name, _valid_to FROM foo FOR ALL VALID_TIME ORDER BY _valid_to NULLS FIRST")
              (map :xt/valid-to))))
  (t/is (=
         [#xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"
          #xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"
          #xt/zoned-date-time "2020-01-02T00:00:00Z[UTC]"
          nil nil nil]
         (->> (xt/q tu/*node* "SELECT name, _valid_to FROM foo FOR ALL VALID_TIME ORDER BY _valid_to NULLS LAST")
              (map :xt/valid-to)))))

(t/deftest select-with-lots-of-commas
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :a "a", :b "b"}]])

  (t/is (= [{}] (xt/q tu/*node* "SELECT FROM foo")))
  (t/is (= [{}] (xt/q tu/*node* "SELECT ,, FROM foo")))
  (t/is (= [{:a "a", :b "b"}] (xt/q tu/*node* "SELECT a,,b FROM foo")))
  (t/is (= [{:a "a"}] (xt/q tu/*node* "SELECT ,,a, FROM foo")))
  (t/is (= [{:a "a", :b "b"}] (xt/q tu/*node* "SELECT a,,b FROM foo")))

  (t/is (= [{:xt/id 1, :a "a", :b "b"}] (xt/q tu/*node* "SELECT *,, FROM foo")))
  (t/is (= [{:xt/id 1, :a "a", :b "b", :xt/valid-from (time/->zdt #inst "2020")}]
           (xt/q tu/*node* "SELECT ,_valid_from,,* FROM foo"))))

(t/deftest multiple-query-tails
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v 1}]
                           [:put-docs :foo {:xt/id 2, :v 1}]
                           [:put-docs :foo {:xt/id 3, :v 2}]
                           [:put-docs :foo {:xt/id 4, :v 3}]
                           [:put-docs :foo {:xt/id 5, :v 4}]])

  (t/is (= [{:v 1, :v-count 2} {:v 2, :v-count 1} {:v 3, :v-count 1} {:v 4, :v-count 1}]
           (xt/q tu/*node* "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            ORDER BY v")))

  (t/is (= [{:v-count 1, :freq 3} {:v-count 2, :freq 1}]
           (xt/q tu/*node* "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            SELECT v_count, COUNT(*) AS freq
                            ORDER BY v_count"))
        "double aggregate")

  (t/is (= [{:v 2} {:v 3} {:v 4} {:v 1}]
           (xt/q tu/*node* "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            SELECT v
                            ORDER BY v_count, v"))
        "ORDER BY sees through outermost SELECT")

  (t/is (= [{:xt/id 1} {:xt/id 3} {:xt/id 4} {:xt/id 5}]
           (xt/q tu/*node* "FROM foo
                            SELECT _id, ROW_NUMBER () OVER (PARTITION BY v ORDER BY _id) row_num
                            WHERE row_num = 1
                            SELECT _id
                            ORDER BY _id"))
        "we can filter on window functions"))

(t/deftest plan-agg-subqueries-before-group-by-3821
  (xt/submit-tx tu/*node* [[:put-docs :docs
                            {:xt/id 1 :name "alan" :sysadmin true}
                            {:xt/id 2 :name "ada" :sysadmin true}
                            {:xt/id 3 :name "claude" :sysadmin false}]])

  (t/is (= 3 (-> (xt/q tu/*node* "SELECT COUNT((SELECT 1)) v FROM docs") first :v)))

  (t/is (= 2 (-> (xt/q tu/*node* "SELECT COUNT(CASE WHEN _id IN (1, 2) THEN _id END) v FROM docs") first :v)))

  (t/is (= [{:sysadmin true, :xt/column-2 ["ada" "alan"]}
            {:sysadmin false, :xt/column-2 ["claude"]}]
           (xt/q tu/*node* "SELECT sysadmin, ARRAY_AGG((SELECT name)) FROM docs GROUP BY sysadmin"))))

(t/deftest test-lateral-with-unnest-4009
  (xt/submit-tx tu/*node* [[:put-docs :nested-table {:xt/id 1 :nest [1 2 3]}]])

  (t/is (= [{:xt/id 1, :nest [1 2 3], :a [1 2 3]}]
           (xt/q tu/*node* "SELECT *
                            FROM nested_table nt,
                            LATERAL (SELECT nt.nest) t(a)")))

  (t/is (= [{:xt/id 1, :nest [1 2 3], :a 1}
            {:xt/id 1, :nest [1 2 3], :a 2}
            {:xt/id 1, :nest [1 2 3], :a 3}]
           (xt/q tu/*node* "SELECT *
                            FROM nested_table nt,
                            LATERAL (FROM UNNEST(nt.nest) AS foo(id)) t(a)"))))

(t/deftest test-period-predicate-optimisations-4054
  (t/testing "plans should only contain optimised versions of period predicates"
    (t/is (=plan-file
           "test-period-predicate-optimisations-4054-variadic-overlaps-2"
           (sql/plan "SELECT foo.name
                    FROM foo, bar, baz
                    WHERE OVERLAPS(foo._valid_time, bar._valid_time, baz._valid_time)"
                     {:table-info {#xt/table foo #{"name"}
                                   #xt/table bar #{"dog"}
                                   #xt/table baz #{"fish"}}})))
    (t/is (=plan-file
           "test-period-predicate-optimisations-4054-variadic-overlaps-3"
           (sql/plan "SELECT foo.name
                    FROM foo, bar
                    WHERE OVERLAPS(foo._valid_time, bar._valid_time)"
                     {:table-info {#xt/table foo #{"name"}
                                   #xt/table bar #{"dog"}
                                   #xt/table baz #{"fish"}}})))
    (t/is (=plan-file
           "test-period-predicate-optimisations-4054-infix-overlaps"
           (sql/plan "SELECT foo.name
                    FROM foo
                    WHERE foo._valid_time OVERLAPS
                    PERIOD(TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"
                     {:table-info {#xt/table foo #{"name"}}})))

    (t/is (=plan-file
           "test-period-predicate-optimisations-4054-contains-with-period-literal"
           (sql/plan "SELECT foo.name
                    FROM foo
                    WHERE foo._valid_time CONTAINS
                    PERIOD(TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"
                     {:table-info {#xt/table foo #{"name"}}})))))

(t/deftest interval-read-write-bug-4066
  (xt/submit-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 1, tx_interval: INTERVAL 'PT1H'};"])

  (t/is (= [{:xt/id 1,
             :tx-interval #xt/interval "PT1H",
             :q-interval #xt/interval "PT1H"}]
           (xt/q tu/*node* "FROM docs SELECT *, INTERVAL 'PT1H' AS q_interval"))))

(t/deftest test-push-selection-down-past-unnest
  (xt/execute-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, vs: [{t: 1, v: 1}, {t: 2, v: 2}]}"])

  (let [q "FROM foo AS f,
           UNNEST(vs) AS uvs(v)
           WHERE f._id < 2
           SELECT f._id, (v).t AS t, (v).v AS v"]
    (t/is (= [{:xt/id 1, :t 1, :v 1} {:xt/id 1, :t 2, :v 2}]
             (xt/q tu/*node* [q])))
    (t/is (=plan-file
           "test-push-selection-down-past-unnest-inner-col-ref"
           (sql/plan q {:table-info {#xt/table foo #{"_id" "vs"}}}))))

  (let [q "FROM foo AS f,
           UNNEST(vs) AS uvs(v)
           WHERE (uvs.v).v < 2
           SELECT f._id, (v).t AS t, (v).v AS v"]
    (t/is (= [{:xt/id 1, :t 1, :v 1}]
             (xt/q tu/*node* [q])))
    (t/is (=plan-file
           "test-push-selection-down-past-unnest-unnested-col-ref"
           (sql/plan q {:table-info {#xt/table foo #{"_id" "vs"}}}))))

  (let [q "FROM foo AS f,
           UNNEST(vs) WITH ORDINALITY AS uvs (v, ord)
           WHERE uvs.ord < 2
           SELECT f._id, (v).t AS t, (v).v AS v"]
    (t/is (= [{:xt/id 1, :t 1, :v 1}]
             (xt/q tu/*node* [q])))
    (t/is (=plan-file
           "test-push-selection-down-past-unnest-ord-col-ref"
           (sql/plan q {:table-info {#xt/table foo #{"_id" "vs"}}})))))

(t/deftest forbid-invalid-columns-insert
  (t/is (anomalous? [:incorrect nil
                     #"Cannot put documents with columns: #\{\"_foo\"\}"]
                    (xt/execute-tx tu/*node* ["INSERT INTO foo(_id, _foo) VALUES(1, 2)"])))

  (t/is (anomalous? [:incorrect nil
                     #"Cannot put documents with columns: #\{\"_system_time\"\}"]
                    (xt/execute-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, _system_time: 2}"])))

  (xt/execute-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1}"])

  (t/is (anomalous? [:incorrect nil
                     #"Cannot INSERT _system_from column"]
                    (xt/execute-tx tu/*node* ["INSERT INTO foo (SELECT *, 1 AS _system_from FROM foo)"])))

  (t/is (anomalous? [:incorrect nil
                     #"Invalid valid times"]
                    (xt/execute-tx tu/*node* ["INSERT INTO docs (_id, _valid_from, _valid_to)
                                         VALUES (1, TIMESTAMP '2021-01-01 00:00:00+00:00', TIMESTAMP '2020-01-01 00:00:00+00:00')"]))))

(t/deftest null-valid-time-insertion-behaves-as-if-non-specified
  (xt/execute-tx tu/*node* ["INSERT INTO docs SELECT 1 _id, NULL _valid_from, NULL _valid_to"])

  (t/is (= [{:xt/id 1, :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs"))))

(t/deftest forbid-invalid-columns-update
  (xt/execute-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1}"])
  (t/is (anomalous? [:incorrect nil
                     #"Cannot UPDATE _valid_from column"]
                    (xt/execute-tx tu/*node* ["UPDATE foo SET _valid_from = 2 WHERE _id = 1"]))))

(t/deftest joins-across-polymorphic-data-4061+4078
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :a 1} {:xt/id 2 :a 1.0}]
                           [:put-docs :docs2 {:xt/id 1 :a 1.0}]])

  (t/is (= #{{:a1 1.0, :a2 1.0} {:a1 1, :a2 1.0}}
           (-> (xt/q tu/*node* "SELECT d1.a a1, d2.a a2 FROM docs d1, docs2 d2 WHERE d1.a = d2.a")
               set))
        "Testing joins with different num types")

  (t/is (= [{:cnt 2}]
           (xt/q tu/*node* "SELECT COUNT(*) cnt FROM docs d1 GROUP BY d1.a"))
        "Testing group-by with different num types")

  (t/is (= [{:xt/id 1 :a 1}]
           (xt/q tu/*node* "SELECT * FROM docs INTERSECT SELECT * FROM docs2")))

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs3 RECORDS
                                  {_id: 1, value: TIMESTAMP '2024-01-01T00:00:00Z'};"]
                            [:put-docs :docs3 {:xt/id 2 :value #xt/date "2024-01-01"}]])

  (t/is (= #{{:id1 1, :id2 1}, {:id1 1, :id2 2},
             {:id1 2, :id2 1}, {:id1 2, :id2 2}}
           (-> (xt/q tu/*node* "FROM docs3 AS d1
                            LEFT OUTER JOIN docs3 AS d2
                            ON d1.value = d2.value + INTERVAL 'PT0M'
                            SELECT d1._id AS id1, d2._id AS id2
                            ORDER BY id1, id2"
                     {:default-tz #xt/zone "UTC"})
               set))
        "Testing joins with differnt temporal types"))

(t/deftest mismatched-columns-in-table-projection-stops-ingestion-4069
  (t/is (anomalous? [:incorrect nil #"Table projection mismatch"]
                    (xt/q tu/*node* "FROM UNNEST([1,2,3]) x(_id, foo)")))

  (t/is (anomalous? [:incorrect nil
                     #"Table projection mismatch for x \(_id,foo\)"]
                    (xt/execute-tx tu/*node* ["INSERT INTO docs (_id) SELECT _id FROM UNNEST([1,2,3]) x (_id, foo)"])))

  (t/is (anomalous? [:incorrect nil #"Table projection mismatch"]
                    (xt/q tu/*node* "FROM generate_series (1, 4) xs (x, foo)")))

  (let [ex-msg (ex-message (t/is (anomalous? [:incorrect nil]
                                             (xt/execute-tx tu/*node* ["INSERT INTO docs (_id) FROM generate_series (1, 4) xs (x, foo)"]))))]
    (t/is (str/includes? ex-msg "Table projection mismatch for xs (x,foo)"))
    (t/is (str/includes? ex-msg "Column count mismatch: expected 1, given 0"))
    (t/is (str/includes? ex-msg "INSERT does not contain mandatory _id column"))))

(t/deftest test-full-outer-join
  (t/testing "uncorelated FOJ"
    (let [q "SELECT foo, bar FROM d1 FULL OUTER JOIN d2 ON foo = bar"]
      (t/is
       (=plan-file "test-full-outer-join-uncorrelated"
                   (sql/plan q
                             {:table-info {#xt/table d1 #{"_id" "bar"} #xt/table d2 #{"_id" "foo"}}})))

      (xt/execute-tx tu/*node* ["INSERT INTO d1 (_id, foo) VALUES (1, 1), (2, 2), (3, 4)"
                                "INSERT INTO d2 (_id, bar) VALUES (1, 1), (2, 2), (3, 3)"])

      (t/is (= [{:foo 2, :bar 2} {:foo 1, :bar 1} {:bar 3} {:foo 4}]
               (xt/q tu/*node* q)))))

  (t/is
   (anomalous? [:incorrect nil
                #"Subqueries are not allowed in this context"]
               (sql/plan "SELECT foo, bar FROM d1 FULL OUTER JOIN d2 ON foo = (SELECT bar)"
                         {:table-info {#xt/table d1 #{"_id" "bar"} #xt/table d2 #{"_id" "foo"}}}))))

(t/deftest order-by-ignored-4193
  (xt/submit-tx tu/*node* ["INSERT INTO docs (_id, col1, w) VALUES (1, 'foo', 'x')"
                           "INSERT INTO docs RECORDS {_id: 2, col1: 'bar', col2: 1, w:'y'}"
                           "INSERT INTO docs RECORDS {_id: 3, col1: 'baz', col2: 1, w:'z'}"])

  (t/is (= [{:t "bar"} {:t "baz"} {:t "foo"}]
           (xt/q tu/*node*
                 "FROM docs d1
                  SELECT d1.col1 t
                  WHERE t IS NOT NULL
                  ORDER BY t asc"))))

(t/deftest test-duplicate-column-projection
  (t/is (anomalous? [:incorrect nil
                     #"Duplicate column projection: a"]
                    (sql/plan "SELECT 1 AS a, 2 AS a" {}))))

(t/deftest test-hashcode-for-tstzrange-4263
  (t/is (= [{:p #xt/tstz-range [#xt/zoned-date-time "2024-01-01T00:00Z" #xt/zoned-date-time "2024-01-02T00:00Z"]}]
           (xt/q tu/*node* "SELECT DISTINCT PERIOD(TIMESTAMP '2024-01-01Z', TIMESTAMP '2024-01-02Z') AS p;"))))

(t/deftest patch-uuid-literal-id-4284
  (xt/execute-tx tu/*node* ["PATCH INTO foo RECORDS {_id: UUID '2f64d726-b528-4897-b3b1-db41cd9e887b'}"])

  (t/is (= [{:xt/id #uuid "2f64d726-b528-4897-b3b1-db41cd9e887b"}]
           (xt/q tu/*node* "SELECT * FROM foo"))))

(t/deftest null->struct-ingestion-stopped-4300
  ;; FIXME #4300
  #_#_#_
  (xt/execute-tx tu/*node* ["INSERT INTO \"test_null_values\" RECORDS {\"metadata\": {\"key\": 'value', \"value\": NULL}, \"_id\": UUID 'b5e2705f-5ba7-46c5-b595-afc96370742d'}"])
  (xt/execute-tx tu/*node* ["UPDATE \"test_null_values\" SET \"metadata\" = NULL WHERE (\"_id\" = UUID 'b5e2705f-5ba7-46c5-b595-afc96370742d')"])

  (xt/q tu/*node* "SELECT * FROM `test_null_values`"))

(t/deftest interval-mdm
  (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 1, tx_interval: INTERVAL 'P1DT1.123456S'}"])
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs RECORDS {_id: 2, tx_interval: ?}" [#xt/interval "P1DT1.123456S"]]])

  (t/is (= [{:xt/id 2, :tx-interval #xt/interval "P1DT1.123456S"}
            {:xt/id 1, :tx-interval #xt/interval "P1DT1.123456S"}]
           (xt/q tu/*node* "FROM docs SELECT *")))

  (t/is (= [{:mdm #xt/interval "P1DT1.123456S",
             :mdn [#xt/interval "P1DT1.123456789S"],
             :mdm-literal #xt/interval "P1DT1.123456S"}]
           (xt/q tu/*node* ["SELECT ? mdm, ? mdn, INTERVAL 'P1DT1.123456S' mdm_literal"
                            #xt/interval "P1DT1.123456S" [#xt/interval "P1DT1.123456789S"]]))))

(defn- compare-decimals [^BigDecimal d1 ^BigDecimal d2]
  (and (= (.scale d1) (.scale d2))
       (= (.unscaledValue d1) (.unscaledValue d2))))

(t/deftest decimal-casting
  (letfn [(q [sql]
            (let [{:keys [res res-type]} (tu/q-sql tu/*node* sql)]
              {:res (:v (first res))
               :res-types (second (first res-type))}))]

    (t/is (= {:res 301.02M, :res-types #xt/type [:decimal 5 2 128]} (q "SELECT 301.02::DECIMAL(5, 2) AS V")))
    (t/is (= {:res 301.0M, :res-types #xt/type [:decimal 5 1 128]} (q "SELECT 301.02::DECIMAL(5, 1) AS V")))
    (t/is (= {:res 301.020M, :res-types #xt/type [:decimal 6 3 128]} (q "SELECT 301.02::DECIMAL(6, 3) AS V")))
    (t/is (= {:res 301M, :res-types #xt/type [:decimal 5 0 128]} (q "SELECT 301.02::DECIMAL(5) AS V")))
    (t/is (= {:res 301.020000000M, :res-types #xt/type [:decimal 64 9 256]} (q "SELECT 301.02::DECIMAL AS V")))
    (t/is (= {:res 301.020000000M, :res-types #xt/type [:decimal 64 9 256]} (q "SELECT '301.02'::DECIMAL AS V")))
    (t/is (anomalous? [:incorrect nil] (q "SELECT '301.02'::DECIMAL(65) AS V"))))

  (t/testing "correct EE behaviour"
    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :d 1.1M} {:xt/id 2 :d 1.02M}]])

    (let [expected-res [2.02M 2.1M]
          {:keys [res res-type]} (tu/q-sql tu/*node* "SELECT d + 1 AS v FROM docs")
          res (map :v res)]

      (t/is (= [["v" #xt/type [:union [:decimal 32 2 128] [:decimal 32 1 128]]]]
               res-type))

      (doseq [[expected result] (map vector expected-res res)]
        (t/is (compare-decimals expected result)
              (str "Expected: " expected ", but got: " result))))

    (let [expected-res [2.02M 2.1M]
          {:keys [res res-type]} (tu/q-sql tu/*node* "SELECT d + 1::decimal(64,1) AS v FROM docs")
          res (map :v res)]

      (t/is (= [["v" #xt/type [:union [:decimal 64 2 256] [:decimal 64 1 256]]]]
               res-type))

      (doseq [[expected result] (map vector expected-res res)]
        (t/is (compare-decimals expected result)
              (str "Expected: " expected ", but got: " result))))))

(t/deftest inline-xtql
  (xt/submit-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, x: 'foo'}"
                           "INSERT INTO bar RECORDS {_id: 1, y: 'bar'}"
                           "INSERT INTO bar RECORDS {_id: 2, y: 'baz'}"])

  (t/is (= [{:xt/id 1, :x "foo"}] (xt/q tu/*node* "XTQL $$(from :foo [*])$$")))
  (t/is (= [{:xt/id 1, :x "foo"}] (xt/q tu/*node* "XTQL $$(from :foo [xt/id x])$$")))
  (t/is (= [{:x "foo"}] (xt/q tu/*node* "XTQL $$ (from :foo [x]) $$")))

  (t/is (= [{:x "foo", :y "bar"}]
           (xt/q tu/*node* "SELECT * EXCLUDE _id
                            FROM foo f1
                              JOIN (XTQL $$(from :bar [_id y])$$) f2
                                USING (_id)")))

  (t/testing "params"
    (t/is (= [{:xt/id 2, :y "baz"}]
             (xt/q tu/*node* ['#(from :bar [{:xt/id %} *]) 2])))

    (t/is (= [{:xt/id 2, :y "baz", :x "x"}]
             (xt/q tu/*node* [(format "FROM (XTQL ($$ %s $$, ?)) t SELECT *, ? AS x"
                                      (pr-str '#(from :bar [{:xt/id %} *])))
                              2 "x"])))

    (with-open [conn (jdbc/get-connection tu/*node*)]
      (t/is (= {:xt/id 2, :y "baz"}
               (jdbc/execute-one! conn [(format "XTQL ($$ %s $$, ?)" (pr-str '#(from :bar [{:xt/id %} *]))) 2]
                                  {:builder-fn xt-jdbc/builder-fn}))
            "XTQL params through PGJDBC"))))

(t/deftest use-parent-left-scope-in-nested-join-table-4131
  (t/is (=plan-file
         "use-parent-left-scope-in-nested-join-table"
         (sql/plan "FROM r1, r2
                    , UNNEST(r1.xs) WITH ORDINALITY AS u1(x1, i1)
                    JOIN UNNEST(r2.xs) WITH ORDINALITY AS u2(x2, i2)
                      ON i1 = i2
                    SELECT i1, i2, x1, x2"
                   {:table-info {#xt/table r1 #{"xs"}
                                 #xt/table r2 #{"xs"}}}))))

(t/deftest inconsistent-patch-behaviour-4448
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO users RECORDS {_id: ?, _valid_from: ?, _valid_to: ?}" [1 #inst "2010" #inst "2040"]]])

  (t/is (anomalous? [:incorrect :xtdb.indexer/invalid-valid-times
                     "Invalid valid times"
                     {:valid-from #xt/instant "2020-01-01T00:00:00Z", :valid-to #xt/instant "2015-01-01T00:00:00Z"
                      :arg-idx 0, :sql "PATCH INTO users FOR PORTION OF VALID_TIME FROM $1 TO $2 RECORDS {_id: 1, foo: 3}", :tx-op-idx 0,
                      :tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}}]
                    (xt/execute-tx tu/*node* [[:sql "PATCH INTO users FOR PORTION OF VALID_TIME FROM ? TO ? RECORDS {_id: 1, foo: 3}" [#inst "2020" #inst "2015"]]])))

  (t/is (= [{:committed false,
             :error #xt/error [:incorrect :xtdb.indexer/invalid-valid-times
                               "Invalid valid times"
                               {:valid-from #xt/instant "2020-01-01T00:00:00Z",
                                :valid-to #xt/instant "2015-01-01T00:00:00Z"
                                :arg-idx 0, :sql "PATCH INTO users FOR PORTION OF VALID_TIME FROM $1 TO $2 RECORDS {_id: 1, foo: 3}", :tx-op-idx 0,
                                :tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"}}]}]
           (xt/q tu/*node* '(from :xt/txs [{:xt/id 1} committed error])))))

(t/deftest temporal-filter-expressions-3306
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :xt/valid-from #inst "2000-01-01" :xt/valid-to #inst "2100-01-01"}]])

  (t/is (= [#:xt{:id 1}]
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME AS OF (CURRENT_TIMESTAMP - INTERVAL 'PT1M')"
                 {:current-time #inst "2020"})))

  (t/is (= [#:xt{:id 1}]
           (xt/q tu/*node* ["SELECT * FROM docs FOR VALID_TIME AS OF (? - INTERVAL 'PT1M')" #inst "2020"])))

  (t/is (= [#:xt{:id 1}]
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME FROM (NOW - INTERVAL 'P1Y') TO CURRENT_TIMESTAMP"
                 {:current-time #inst "2020"})))

  (t/is (= [#:xt{:id 1}]
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME BETWEEN CURRENT_TIMESTAMP AND (CURRENT_TIMESTAMP + INTERVAL 'P1Y')"
                 {:current-time #inst "2020"})))

  (t/is (= [#:xt{:id 1}]
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME AS OF NULL"
                 {:current-time #inst "2020"})))

  (t/is (= []
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME AS OF NULL"
                 {:current-time #inst "2100"})))

  (t/is (= []
           (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME AS OF (NOW - INTERVAL 'PT1M')"
                 {:current-time #inst "2120"}))
        "Interval outside period"))

(t/deftest order-by-without-select-bug-4560
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :col 1, :d #xt/zdt "2024-01-01Z"}
                             {:xt/id 2, :col 2, :d #xt/zdt "2024-01-03Z"}
                             {:xt/id 3, :col 1, :d #xt/zdt "2024-01-02Z"}]])

  (t/is (= [{:xt/id 3, :col 1, :d #xt/zdt "2024-01-02T00:00Z"}
            {:xt/id 1, :col 1, :d #xt/zdt "2024-01-01T00:00Z"}]
           (xt/q tu/*node*
                 "FROM docs
                  WHERE col = 1
                  ORDER BY d DESC")))

  (t/is (= [{:xt/id 2, :col 2, :d #xt/zdt "2024-01-03T00:00Z"}
            {:xt/id 3, :col 1, :d #xt/zdt "2024-01-02T00:00Z"}
            {:xt/id 1, :col 1, :d #xt/zdt "2024-01-01T00:00Z"}]
           (xt/q tu/*node*
                 "FROM docs
                  ORDER BY d DESC")))

  (t/is (= [{:xt/id 3, :col 1, :d #xt/zdt "2024-01-02T00:00Z"}
            {:xt/id 1, :col 1, :d #xt/zdt "2024-01-01T00:00Z"}]
           (xt/q tu/*node*
                 "FROM docs
                  WHERE col = 1
                  SELECT *
                  ORDER BY d DESC"))))

(t/deftest update-with-many-params-4803
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id "id"}]])

  (xt/execute-tx tu/*node* [["UPDATE docs SET c1=?,c2=?,c3=?,c4=?,c5=?,c6=?,c7=? WHERE _id=?"
                             1 2 3 4 5 6 7 "id"]])

  (t/is (= [{:xt/id "id", :c1 1, :c2 2, :c3 3, :c4 4, :c5 5, :c6 6, :c7 7}]
           (xt/q tu/*node* "FROM docs")))

  (xt/execute-tx tu/*node* [["UPDATE docs SET c1=?,c2=?,c3=?,c4=?,c5=?,c6=?,c7=?,c8=?,c9=? WHERE _id=?"
                             1 2 3 4 5 6 7 8 9 "id"]])

  (t/is (= [{:xt/id "id", :c1 1, :c2 2, :c3 3, :c4 4, :c5 5, :c6 6, :c7 7, :c8 8, :c9 9}]
           (xt/q tu/*node* "FROM docs"))))

(t/deftest scrambled-update-4803
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id "id"}]])
  (xt/execute-tx tu/*node* [["UPDATE docs SET c1=?,c2=?,c3=?,c4=?,c5=?,c6=?,c7=?,c8=?,c9=? WHERE _id='id'"
                             1 2 3 4 5 6 7 8 9]])
  (t/is (= [{:xt/id "id", :c1 1, :c2 2, :c3 3, :c4 4, :c5 5, :c6 6, :c7 7, :c8 8, :c9 9}]
           (xt/q tu/*node* "FROM docs"))))


(t/deftest subquery-in-exists-incorrect-table-name-4697
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :col1 "bar" :col2 "baz"} {:xt/id 2 :col1 "foo" :col2 "bar"}]])

  (t/is (= [{:e false} {:e true}]
           (xt/q tu/*node* "FROM docs AS docs_outer
                            SELECT
                              EXISTS(
                                FROM (
                                  FROM docs AS docs_inner
                                  WHERE docs_outer.col1 = docs_inner.col2
                                ) AS d
                                SELECT 1
                              ) AS e"))))

(t/deftest use-same-leg-name-for-varbinary-4751
  (binding [c/*ignore-signal-block?* true]
    (xt/execute-tx tu/*node* [[:patch-docs :docs {:xt/id 1 :d [(ByteBuffer/allocate 0) false]}]])

    (t/is (not-empty (xt/q tu/*node* "SELECT * FROM docs")))))

(t/deftest test-all-quantified-comparison
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :col1 8} {:xt/id 2 :col1 9}]])

  (t/is (= [{:result false}]
           (xt/q tu/*node* "SELECT 8 = ALL(SELECT col1 FROM docs) AS result")))

  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :col1 8} {:xt/id 2 :col1 8}]])
  (t/is (= [{:result true}]
           (xt/q tu/*node* "SELECT 8 = ALL(SELECT col1 FROM docs) AS result")))

  (t/is (= [{:result true}]
           (xt/q tu/*node* "SELECT 10 <> ALL(SELECT col1 FROM docs) AS result")))

  (t/is (= [{:result false}]
           (xt/q tu/*node* "SELECT 8 <> ALL(SELECT col1 FROM docs) AS result")))

  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :col1 5} {:xt/id 2 :col1 7}]])
  (t/is (= [{:result true}]
           (xt/q tu/*node* "SELECT 10 > ALL(SELECT col1 FROM docs) AS result")))

  (t/is (= [{:result false}]
           (xt/q tu/*node* "SELECT 6 > ALL(SELECT col1 FROM docs) AS result")))

  (t/is (= [{:result true}]
           (xt/q tu/*node* "SELECT 5 = ANY(SELECT col1 FROM docs) AS result")))

  (t/is (= [{:result false}]
           (xt/q tu/*node* "SELECT 10 = ANY(SELECT col1 FROM docs) AS result"))))

(t/deftest update-interval-with-months-5070
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :i #xt/interval "P12MT1S"}]])
  (t/is (= [{:xt/id 1, :i #xt/interval "P12MT1S"}] (xt/q tu/*node* "FROM docs")))

  (t/testing "update document with same interval (with months) - update should get de-duplicated"
    (xt/execute-tx tu/*node* [["UPDATE docs SET i = ? WHERE _id = ?"  #xt/interval "P12MT1S" 1]])
    (t/is (= 1 (count (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME ALL")))))

  (t/testing "update document with different interval (with months) - update should go through"
    (xt/execute-tx tu/*node* [["UPDATE docs SET i = ? WHERE _id = ?"  #xt/interval "P12MT2S" 1]])
    (t/is (= 2 (count (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME ALL"))))
    (t/is (= [{:xt/id 1, :i #xt/interval "P12MT2S"}] (xt/q tu/*node* "FROM docs")))))

(t/deftest update-dedup-structs-5113
  (t/testing "update with identical struct should be deduplicated"
    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a {:x 0}}]])
    (t/is (= [{:xt/id 1, :a {:x 0}}] (xt/q tu/*node* "FROM docs")))

    (xt/execute-tx tu/*node* [[:sql "UPDATE docs SET a = ? WHERE _id = 1" [{:x 0}]]])
    (t/is (= [{:xt/id 1, :a {:x 0}}] (xt/q tu/*node* "FROM docs")))
    (t/is (= 1 (count (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME ALL"))))))