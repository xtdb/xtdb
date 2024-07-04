(ns xtdb.sql-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn plan-sql
  ([sql opts] (sql/compile-query sql opts))
  ([sql] (plan-sql sql {})))

(def regen-expected-files? false) ;; <<no-commit>>

(defmethod t/assert-expr '=plan-file [msg form]
  `(let [exp-plan-file-name# ~(nth form 1)
         exp-plan-file-path# (format "xtdb/sql/plan_test_expectations/%s.edn" exp-plan-file-name#)
         actual-plan# ~(nth form 2)]
     (binding [*print-namespace-maps* false]
       (if-let [exp-plan-file# (io/resource exp-plan-file-path#)]
         (let [exp-plan# (read-string (slurp exp-plan-file#))
               result# (= exp-plan# actual-plan#)]
           (if result#
             (t/do-report {:type :pass
                           :message ~msg
                           :expected (list '~'= exp-plan-file-name# actual-plan#)
                           :actual (list '~'= exp-plan# actual-plan#)})
             (do
               (when regen-expected-files?
                 (spit (io/resource exp-plan-file-path#) (with-out-str (clojure.pprint/pprint actual-plan#))))
               (t/do-report {:type :fail
                             :message ~msg
                             :expected (list '~'= exp-plan-file-name# actual-plan#)
                             :actual (list '~'not (list '~'= exp-plan# actual-plan#))})))
           result#)
         (if regen-expected-files?
           (do
             (spit
              (str (io/resource "xtdb/sql/plan_test_expectations/") exp-plan-file-name# ".edn")
              (with-out-str (clojure.pprint/pprint actual-plan#))))
           (t/do-report
            {:type :error, :message "Missing Expectation File"
             :expected exp-plan-file-path#  :actual (Exception. "Missing Expectation File")}))))))

(deftest test-basic-queries
  (t/is (=plan-file
          "basic-query-1"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-1"
          (plan-sql "FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960 SELECT si.movieTitle"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-2"
          (plan-sql "FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950 SELECT si.movieTitle"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-3"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-4"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-5"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.`year` = m.movieYear"
                    {:table-info {"movie" #{"title" "movie_year"}
                                  "stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-6"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.`year` = m.movieYear"
                    {:table-info {"movie" #{"title" "movie_year"}
                                  "stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-9"
          (plan-sql "SELECT me.name, SUM(m.`length`) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.`year`) < 1930"
                    {:table-info {"movie_exec" #{"name" "cert"}
                                  "movie" #{"producer" "year" "length"}}})))

  (t/is (=plan-file
          "basic-query-10"
          (plan-sql "SELECT SUM(m.`length`) FROM Movie AS m"
                    {:table-info {"movie" #{"length"}}})))

  (t/is (=plan-file
          "basic-query-11"
          (plan-sql "SELECT * FROM StarsIn AS si(name)"
                    {:table-info {"stars_in" #{"name" "title"}}})))

  (t/is (=plan-file
          "basic-query-11"
          (plan-sql "FROM StarsIn AS si(name)"
                    {:table-info {"stars_in" #{"name" "title"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-12"
          (plan-sql "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-12"
          (plan-sql "FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
                    {:table-info {"stars_in" #{"name"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-13"
         (plan-sql "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname"
                   {:table-info {"stars_in" #{"name" "lastname"}}}))))

(t/deftest test-values
  (t/is (=plan-file
          "basic-query-32"
          (plan-sql "VALUES (1, 2), (3, 4)")))

  (t/is (=plan-file
          "basic-query-33"
           (plan-sql "VALUES 1, 2"))))

(t/deftest test-is-null
  (t/is (=plan-file
          "basic-query-35"
          (plan-sql "SELECT * FROM t1 AS t1(a) WHERE t1.a IS NULL"
                    {:table-info {"t1" #{"a"}}})))

  (t/is (=plan-file
          "basic-query-35"
          (plan-sql "FROM t1 AS t1(a) WHERE t1.a IS NULL"
                    {:table-info {"t1" #{"a"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-36"
          (plan-sql "SELECT * FROM t1 WHERE a IS NOT NULL"
                    {:table-info {"t1" #{"a"}}})))

  (t/is (=plan-file
          "basic-query-36"
          (plan-sql "FROM t1 WHERE a IS NOT NULL"
                    {:table-info {"t1" #{"a"}}}))
        "implicit SELECT *"))

(t/deftest test-case
  (t/is (=plan-file
         "basic-query-34"
         (plan-sql "SELECT CASE t1.a + 1 WHEN t1.b THEN 111 WHEN t1.c THEN 222 WHEN t1.d THEN 333 WHEN t1.e THEN 444 ELSE 555 END,
                    CASE WHEN t1.a < t1.b - 3 THEN 111 WHEN t1.a <= t1.b THEN 222 WHEN t1.a < t1.b+3 THEN 333 ELSE 444 END,
                    CASE t1.a + 1 WHEN t1.b, t1.c THEN 222 WHEN t1.d, t1.e + 1 THEN 444 ELSE 555 END FROM t1"
                   {:table-info {"t1" #{"a" "b" "c" "d" "e"}}})))

  (t/is (=plan-file
         "basic-query-37"
         (plan-sql "SELECT NULLIF(t1.a, t1.b) FROM t1"
                   {:table-info {"t1" #{"a" "b"}}}))))

(t/deftest test-basic-set-operators
  (t/is (=plan-file
         "basic-query-14"
         (plan-sql "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
                   {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
         "basic-query-15"
         (plan-sql "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
                   {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
         "basic-query-16"
         (plan-sql "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
                   {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
         "basic-query-17"
         (plan-sql "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
                   {:table-info {"stars_in" #{"name"}}})))

  #_ ; TODO should rename the RHS col
  (t/is (=plan-file
         "basic-query-18"
         (plan-sql "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
                   {:table-info {"stars_in" #{"movie_title" "name"}}})))

  #_ ; TODO order-by over union shouldn't create eobrs
  (t/is (=plan-file
         "basic-query-19"
         (plan-sql "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
                   {:table-info {"stars_in" #{"name"}}}))))

(t/deftest test-order-by-limit-offset
  (t/is (=plan-file
          "basic-query-20"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-20"
          (plan-sql "FROM StarsIn AS si SELECT si.movieTitle FETCH FIRST 10 ROWS ONLY"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-21"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-21"
          (plan-sql "FROM StarsIn AS si SELECT si.movieTitle OFFSET 5 ROWS"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-22"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 LIMIT 10"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-22"
          (plan-sql "FROM StarsIn AS si SELECT si.movieTitle OFFSET 5 LIMIT 10"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-23"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-24"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-25"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  #_ ; TODO this is an error
  (t/is (=plan-file
          "basic-query-26"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.\"year\" = 'foo' DESC, movieTitle"
                    {:table-info {"stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-27"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.`year`"
                    {:table-info {"stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-28"
           (plan-sql "SELECT si.`year` = 'foo' FROM StarsIn AS si ORDER BY si.`year` = 'foo'"
                     {:table-info {"stars_in" #{"year"}}}))))

(t/deftest test-unnest
  (t/is (=plan-file
          "basic-query-29"
          (plan-sql "SELECT film FROM StarsIn AS si, UNNEST(si.films) AS film(film)"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-30"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-30"
          (plan-sql "FROM StarsIn si, UNNEST(films) AS film"
                    {:table-info {"stars_in" #{"films"}}}))

        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-31"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-31"
          (plan-sql "FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                    {:table-info {"stars_in" #{"films"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "unnest-query-1"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film(film, film_ord)"
                    {:table-info {"stars_in" #{"films"}}})))

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

(deftest test-cross-join
  (t/is (=plan-file
         "cross-join-1"
         (plan-sql "SELECT * FROM a CROSS JOIN b"
                   {:table-info {"a" #{"a1" "a2"}
                                 "b" #{"b1"}}})))
  (t/is (=plan-file
         "cross-join-2"
         (plan-sql "SELECT c1 FROM a, b CROSS JOIN c"
                   {:table-info {"a" #{"a1" "a2"}
                                 "b" #{"b1"}
                                 "c" #{"c1"}}}))))

(deftest test-named-columns-join
  (t/is (=plan-file
         "basic-query-7"
         (plan-sql "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)"
                   {:table-info {"movie" #{"title"}
                                 "stars_in" #{"title"}}})))

  (t/is (=plan-file
         "basic-query-8"
         (plan-sql "SELECT si.title FROM Movie AS m LEFT OUTER JOIN StarsIn AS si USING (title)"
                   {:table-info {"movie" #{"title"}
                                 "stars_in" #{"title"}}}))))

(deftest test-natural-join
  (t/is (=plan-file
         "natural-join-1"
         (plan-sql "SELECT si.title, m.`length`, si.films FROM Movie AS m NATURAL JOIN StarsIn AS si"
                   {:table-info {"movie" #{"title" "length"}
                                 "stars_in" #{"title" "films"}}})))

  (t/is (=plan-file
         "natural-join-2"
         (plan-sql "SELECT si.title, m.`length`, si.films FROM Movie AS m NATURAL RIGHT OUTER JOIN StarsIn AS si"
                   {:table-info {"movie" #{"title" "length"}
                                 "stars_in" #{"title" "films"}}}))))


;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (=plan-file
            "scalar-subquery-in-select-1"
            (plan-sql "SELECT (1 = (SELECT bar FROM foo)) AS some_column FROM x WHERE y = 1"
                      {:table-info {"x" #{"y"}
                                    "foo" #{"bar"}}})))

    (t/is (=plan-file
            "scalar-subquery-in-select-2"
            (plan-sql "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
                      {:table-info {"x" #{"y"}
                                    "foo" #{"bar"}}}))))

  (t/testing "Scalar subquery in WHERE"
    (t/is (=plan-file
            "scalar-subquery-in-where"
            (plan-sql "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"
                      {:table-info {"x" #{"y"}
                                    "foo" #{"bar"}}}))))

  (t/testing "Correlated scalar subquery in SELECT"
    (t/is (=plan-file
            "correlated-scalar-subquery-in-select"
            (plan-sql "SELECT (1 = (SELECT bar = z FROM foo)) AS some_column FROM x WHERE y = 1"
                      {:table-info {"x" #{"y" "z"}
                                    "foo" #{"bar"}}}))))

  (t/testing "EXISTS in WHERE"
    (t/is (=plan-file
            "exists-in-where"
             (plan-sql "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10.0"
                       {:table-info {"x" #{"y" "z"}
                                     "y" #{"z"}}}))))

  (t/testing "EXISTS as expression in SELECT"
    (t/is (=plan-file
            "exists-as-expression-in-select"
            (plan-sql "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "NOT EXISTS in WHERE"
    (t/is (=plan-file
            "not-exists-in-where"
            (plan-sql "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "IN in WHERE"
    (t/is (=plan-file
            "in-in-where-select"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}})))

    (t/is (=plan-file
            "in-in-where-set"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (1, 2)"
                      {:table-info {"x" #{"y" "z"}}}))))

  (t/testing "NOT IN in WHERE"
    (t/is (=plan-file
            "not-in-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ALL in WHERE"
    (t/is (=plan-file
            "all-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ANY in WHERE"
    (t/is (=plan-file
            "any-in-where"
            (plan-sql "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ALL as expression in SELECT"
    (t/is (=plan-file
            "all-as-expression-in-select"
            (plan-sql "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "LATERAL derived table"
    (t/is (=plan-file
            "lateral-derived-table-1"
            (plan-sql "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
                      {:table-info {"x" #{"y"}
                                    "z" #{"z"}}})))

    (t/is (=plan-file
            "lateral-derived-table-2"
            (plan-sql "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"
                      {:table-info {"z" #{"z"}}}))))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (t/is (=plan-file
            "decorrelation-1"
            (plan-sql "SELECT c.custkey FROM customer c
                      WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)"
                      {:table-info {"customer" #{"custkey"}
                                    "orders" #{"custkey" "totalprice"}}})))

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (t/is (=plan-file
            "decorrelation-2"
            (plan-sql "SELECT * FROM customers AS customers(country, custno)
                      WHERE customers.country = 'Mexico' AND
                      EXISTS (SELECT * FROM orders AS orders(custno) WHERE customers.custno = orders.custno)"
                      {:table-info {"customers" #{"country" "custno"}
                                    "orders" #{"custno"}}})))

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (t/is (=plan-file
            "decorrelation-3"
            (plan-sql "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
                      FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)"
                      {:table-info {"customers" #{"name" "custno" "country"}
                                    "orders" #{"custno"}
                                    "salesp" #{"country"}}})))

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (t/is (=plan-file
            "decorrelation-4"
            (plan-sql "SELECT s.name, e.course
                      FROM students s, exams e
                      WHERE s.id = e.sid AND
                      e.grade = (SELECT MIN(e2.grade)
                      FROM exams e2
                      WHERE s.id = e2.sid)"
                      {:table-info {"students" #{"id" "name"}
                                    "exams" #{"sid" "grade" "course"}}})))

    (t/is (=plan-file
            "decorrelation-5"
            (plan-sql
              "SELECT s.name, e.course
              FROM students s, exams e
              WHERE s.id = e.sid AND
              (s.major = 'CS' OR s.major = 'Games Eng') AND
              e.grade >= (SELECT AVG(e2.grade) + 1
              FROM exams e2
              WHERE s.id = e2.sid OR
              (e2.curriculum = s.major AND
              s.\"year\" > e2.\"date\"))"
              {:table-info {"students" #{"id" "major" "name" "year"}
                            "exams" #{"sid" "grade" "course" "curriculum" "date"}}})))


    (t/testing "Subqueries in join conditions"

      (->> "uncorrelated subquery"
           (t/is (=plan-file
                   "subquery-in-join-uncorrelated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo)"
                             {:table-info {"foo" #{"a" "b"}
                                           "bar" #{"c"}}}))))

      (->> "correlated subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"
                             {:table-info {"foo" #{"a" "b"}
                                           "bar" #{"b" "c"}}}))))

      ;; TODO unable to decorr, need to be able to pull the select over the max-1-row
      ;; although should be able to do this now, no such thing as max-1-row any more
      (->> "correlated equalty subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-equality-subquery"
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)"
                              {:table-info {"foo" #{"a" "b"}
                                            "bar" #{"b" "c"}}})))))))

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

(deftest non-semi-join-subquery-optimizations-test
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-1"
          (plan-sql "select f.a from foo f where f.a in (1,2) or f.b = 42"
                    {:table-info {"foo" #{"a" "b"}}}))
        "should not be decorrelated")
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-2"
          (plan-sql "select f.a from foo f where true = (EXISTS (SELECT foo.c from foo))"
                    {:table-info {"foo" #{"a" "c"}}}))
        "should be decorrelated as a cross join, not a semi/anti join"))

(deftest multiple-ins-in-where-clause
  (t/is (=plan-file
          "multiple-ins-in-where-clause"
          (plan-sql "select f.a from foo f where f.a in (1,2) AND f.a = 42 AND f.b in (3,4)"
                    {:table-info {"foo" #{"a" "b"}}}))))

#_ ; FIXME broken
(deftest deeply-nested-correlated-query
  (t/is (=plan-file
          "deeply-nested-correlated-query"
          (plan-sql "SELECT R1.A, R1.B
                    FROM R R1, S
                    WHERE EXISTS
                    (SELECT R2.A, R2.B
                    FROM R R2
                    WHERE R2.A = R1.B AND EXISTS
                    (SELECT R3.A, R3.B
                    FROM R R3
                    WHERE R3.A = R2.B AND R3.B = S.C))"
                    {:table-info {"r" #{"a" "b"}, "s" #{"c"}}}))))

(t/deftest test-array-element-reference-107
  (t/is (=plan-file
          "test-array-element-reference-107-1"
          (plan-sql "SELECT u.a[1] AS first_el FROM u"
                    {:table-info {"u" #{"a"}}})))

  (t/is (=plan-file
          "test-array-element-reference-107-2"
          (plan-sql "SELECT u.b[u.a[1]] AS dyn_idx FROM u"
                    {:table-info {"u" #{"a" "b"}}}))))

(t/deftest test-current-time-111
  (t/is (=plan-file
          "test-current-time-111"
          (plan-sql "
                    SELECT u.a,
                    CURRENT_TIME, CURRENT_TIME(2),
                    CURRENT_DATE,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(4),
                    LOCALTIME, LOCALTIME(6),
                    LOCALTIMESTAMP, LOCALTIMESTAMP(9),
                    END_OF_TIME, END_OF_TIME()
                    FROM u"
                    {:table-info {"u" #{"a"}}}))))

(t/deftest test-dynamic-parameters-103
  (t/is (=plan-file
         "test-dynamic-parameters-103-1"
         (plan-sql "SELECT foo.a FROM foo WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {"foo" #{"a" "b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-2"
         (plan-sql "SELECT foo.a
                    FROM foo, (SELECT bar.b FROM bar WHERE bar.c = ?) bar (b)
                    WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {"foo" #{"a" "b" "c"}
                                 "bar" #{"b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-subquery-project"
         (plan-sql "SELECT t1.col1, (SELECT ? FROM bar WHERE bar.col1 = 4) FROM t1"
                   {:table-info {"t1" #{"col1"}
                                 "bar" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-top-level-project"
         (plan-sql "SELECT t1.col1, ? FROM t1"
                   {:table-info {"t1" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-set-value"
         (plan-sql "UPDATE t1 SET col1 = ?"
                   {:table-info {"t1" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-table-values"
         (plan-sql "SELECT bar.foo FROM (VALUES (?)) AS bar(foo)"
                   {:table-info {"bar" #{"foo"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-app-time"
         (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                   {:table-info {"users" #{"first_name" "id"}}}))))

(t/deftest test-dynamic-temporal-filters-3068
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME AS OF ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-from-to"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME FROM ? TO ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-between"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME BETWEEN ? AND ?"
                {:table-info {"foo" #{"bar"}}}))))
  
  (t/testing "AS OF SYSTEM TIME"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of-system-time"
      (plan-sql "SELECT bar FROM foo FOR SYSTEM_TIME AS OF ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "using dynamic AS OF in a query"
    (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015"}
                              {:xt/id :matthew}]
                             [:put-docs {:into :docs, :valid-from #inst "2018"}
                              {:xt/id :mark}]])
    (t/is
     (= [{:xt/id :matthew}]
        (xt/q tu/*node* "SELECT docs._id FROM docs FOR VALID_TIME AS OF ?" {:args [#inst "2016"]})))))

(t/deftest test-order-by-null-handling-159
  (t/is (=plan-file
         "test-order-by-null-handling-159-1"
         (plan-sql "SELECT a FROM foo ORDER BY a NULLS FIRST"
                   {:table-info {"foo" #{"a"}}})))

  (t/is (=plan-file
         "test-order-by-null-handling-159-2"
         (plan-sql "SELECT a FROM foo ORDER BY a NULLS LAST"
                   {:table-info {"foo" #{"a"}}}))))

(t/deftest test-arrow-table
  (t/is (=plan-file
          "test-arrow-table-1"
          (plan-sql "SELECT foo.a FROM ARROW_TABLE('test.arrow') AS foo (a)")))

  (t/is (=plan-file
          "test-arrow-table-2"
          (plan-sql "SELECT * FROM ARROW_TABLE('test.arrow') AS foo (a, b)"))))

(deftest test-projects-that-matter-are-maintained
  (t/is (=plan-file
          "projects-that-matter-are-maintained"
          (plan-sql
            "SELECT customers.id
            FROM customers
            UNION
            SELECT o.id
            FROM
            (SELECT orders.id, orders.product
            FROM orders) AS o"
            {:table-info {"customers" #{"id"}
                          "orders" #{"id" "product"}}}))))

(deftest test-semi-and-anti-joins-are-pushed-down
  (t/is (=plan-file
          "test-semi-and-anti-joins-are-pushed-down"
          (plan-sql
            "SELECT t1.a1
            FROM t1, t2, t3
            WHERE t1.b1 in (532,593)
            AND t2.b1 in (808,662)
            AND t3.c1 in (792,14)
            AND t1.a1 = t2.a2"
            {:table-info {"t1" #{"a1" "b1"}
                          "t2" #{"b1" "a2"}
                          "t3" #{"c1"}}}))))

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

(deftest test-group-by-with-projected-column-in-expr
  (t/is (=plan-file
          "test-group-by-with-projected-column-in-expr"
          (plan-sql
            "SELECT foo.a - 4 AS bar
            FROM foo
            GROUP BY foo.a"
            {:table-info {"foo" #{"a"}}})))

  (t/is (=plan-file
          "test-group-by-with-projected-column-in-expr-2"
          (plan-sql
            "SELECT SUM(foo.a - 4) AS bar
            FROM foo "
            {:table-info {"foo" #{"a"}}}))))

(deftest test-array-subqueries
  (t/are [file q]
    (=plan-file file (plan-sql q {:table-info {"a" #{"a" "b"}, "b" #{"b1" "b2"}}}))

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
           (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = a.b) FROM a where a.a = 42"))))

(t/deftest test-expr-in-equi-join
  (t/is
    (=plan-file
      "test-expr-in-equi-join-1"
      (plan-sql "SELECT a FROM a JOIN bar b ON a+1 = b+1"
                {:table-info {"a" #{"a"}, "bar" #{"b"}}})))
  (t/is
    (=plan-file
      "test-expr-in-equi-join-2"
      (plan-sql "SELECT a FROM a JOIN bar b ON a = b+1"
                {:table-info {"a" #{"a"}, "bar" #{"b"}}}))))

(deftest push-semi-and-anti-joins-down-test
  ;;semi-join was previously been pushed down below the cross join
  ;;where the cols it required weren't in scope
  ;; TODO I think this should be decorr'able?
  (t/is
   (=plan-file
    "push-semi-and-anti-joins-down"
    (plan-sql "SELECT x.foo
               FROM x, y
               WHERE EXISTS (
                       SELECT z.bar
                       FROM z
                       WHERE z.bar = x.foo AND z.baz = y.biz
                     )"
              {:table-info {"x" #{"foo"}
                            "y" #{"biz"}
                            "z" #{"bar" "baz"}}}))))

(deftest test-system-time-queries
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "system-time-as-of"
      (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {"foo" #{"bar"}}})))

    (t/is
     (=plan-file
      "system-time-as-of"
      (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "system-time-from-a-to-b"
      (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "system-time-between-subquery"
      (plan-sql "SELECT (SELECT 4 FROM t1 FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00') FROM t2"
                {:table-info {"t1" #{}, "t2" #{}}}))))

  (t/is
   (=plan-file
    "system-time-between-lateraly-derived-table"
    (plan-sql "SELECT x.y, y.z FROM x FOR SYSTEM_TIME AS OF DATE '3001-01-01',
                  LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y"
              {:table-info {"z" #{"z"}, "x" #{"y"}}}))))

(deftest setting-default-system-time
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "system-time-as-of"
      (plan-sql "SETTING DEFAULT SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00' SELECT foo.bar FROM foo"
                {:table-info {"foo" #{"bar"}}})))

    (t/is
     (=plan-file
      "system-time-as-of"
      (plan-sql "SETTING DEFAULT SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00' SELECT foo.bar FROM foo"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "system-time-from-a-to-b"
      (plan-sql "SETTING DEFAULT SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00' SELECT foo.bar FROM foo"
                {:table-info {"foo" #{"bar"}}}))))

  (t/is
   (=plan-file
    "system-time-between-lateraly-derived-table"
    (plan-sql "SETTING DEFAULT SYSTEM_TIME AS OF DATE '3001-01-01'
                 SELECT x.y, y.z
                 FROM x, LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y"
              {:table-info {"z" #{"z"}, "x" #{"y"}}}))))

(deftest test-valid-time-period-spec-queries
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "valid-time-period-spec-as-of"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "valid-time-period-spec-from-to"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-time-period-spec-between"
      (plan-sql "SELECT 4 FROM t1 FOR VALID_TIME BETWEEN TIMESTAMP '3000-01-01 00:00:00+00:00' AND DATE '3001-01-01'"
                {:table-info {"t1" #{}}})))))

(deftest test-valid-and-system-time-period-spec-queries
  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (plan-sql "SELECT 4
               FROM t1
                  FOR SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
                  FOR VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'"
              {:table-info {"t1" #{}}})))

  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (plan-sql "SETTING DEFAULT VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01',
                       DEFAULT SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
               SELECT 4 FROM t1"
              {:table-info {"t1" #{}}})))

  (t/is
   (=plan-file
    "valid-and-system-time-period-spec-between"
    (plan-sql "SETTING DEFAULT SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01',
                       DEFAULT VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'
               SELECT 4 FROM t1"
              {:table-info {"t1" #{}}}))))

(deftest test-multiple-references-to-temporal-cols
  (t/is
   (=plan-file
    "multiple-references-to-temporal-cols"
    (plan-sql "SELECT foo._valid_from, foo._valid_to, foo._system_from, foo._system_to
                FROM foo FOR SYSTEM_TIME FROM DATE '2001-01-01' TO DATE '2002-01-01'
                WHERE foo._valid_from = 4 AND foo._valid_to > 10
                AND foo._system_from = 20 AND foo._system_to <= 23
                AND foo.VALID_TIME OVERLAPS PERIOD (DATE '2000-01-01', DATE '2004-01-01')"
              {:table-info {"foo" {}}}))))

(deftest test-sql-insert-plan
  (t/is (=plan-file "test-sql-insert-plan-1"
                    (plan-sql "INSERT INTO users (_id, name, _valid_from) VALUES (?, ?, ?)")))

  (t/is (=plan-file "test-sql-insert-plan-2"
                    (plan-sql "INSERT INTO users
                               SELECT bar._id, bar.name, bar._valid_from
                               FROM (VALUES (?, ?, ?)) AS bar(_id, name, _valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-3"
                    (plan-sql "INSERT INTO users (_id, name, _valid_from)
                               SELECT bar._id, bar.name, bar._valid_from
                               FROM (VALUES (?, ?, ?)) AS bar(_id, name, _valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-309"
                    (plan-sql "INSERT INTO customer (_id, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        "#309")

  (t/is (=plan-file "test-sql-insert-plan-398"
                    (plan-sql "INSERT INTO foo (_id, _valid_from) VALUES ('foo', DATE '2018-01-01')"))))

(deftest test-sql-delete-plan
  (t/is (=plan-file "test-sql-delete-plan"
                    (plan-sql "DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' TO END_OF_TIME AS u WHERE u.id = ?"
                              {:table-info {"users" #{"id"}}}))))

(deftest test-sql-erase-plan
  (t/is (=plan-file "test-sql-erase-plan"
                    (plan-sql "ERASE FROM users AS u WHERE u.id = ?"
                              {:table-info {"users" #{"id"}}}))))

(deftest test-sql-update-plan
  (t/is (=plan-file "test-sql-update-plan"
                    (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' TO END_OF_TIME AS u SET first_name = 'Sue' WHERE u.id = ?"
                              {:table-info {"users" #{"id" "first_name" "last_name"}}})))

  (t/is (=plan-file "test-sql-update-plan-with-column-references"
                    (plan-sql "UPDATE foo FOR ALL VALID_TIME SET bar = foo.baz"
                              {:table-info {"foo" #{"bar" "baz" "quux"}}})))

  (t/is (=plan-file "test-sql-update-plan-with-period-references"
                    (plan-sql "UPDATE foo FOR ALL VALID_TIME SET bar = (foo.SYSTEM_TIME OVERLAPS foo.VALID_TIME)"
                              {:table-info {"foo" #{"bar" "baz"}}}))))

(deftest dml-target-table-aliases
  (let [opts {:table-info {"t1" #{"col1"}}}]
    (t/testing "UPDATE"
      (t/is (=plan-file "update-target-table-aliases-1"
                        (plan-sql "UPDATE t1 AS u SET col1 = 30" opts)))

      (t/is (=plan-file "update-target-table-aliases-1"
                        (plan-sql "UPDATE t1 u SET col1 = 30" opts)))

      (t/is (=plan-file "update-target-table-aliases-2"
                        (plan-sql "UPDATE t1 SET col1 = 30" opts))))

    (t/testing "DELETE"
      (t/is (=plan-file "delete-target-table-aliases-1"
                        (plan-sql "DELETE FROM t1 AS u WHERE u.col1 = 30" opts)))

      (t/is (=plan-file "delete-target-table-aliases-1"
                        (plan-sql "DELETE FROM t1 u WHERE u.col1 = 30" opts)))

      (t/is (=plan-file "delete-target-table-aliases-2"
                        (plan-sql "DELETE FROM t1 WHERE t1.col1 = 30" opts))))))

(deftest test-system-time-period-predicate
  (t/is
    (=plan-file
      "test-system-time-period-predicate-full-plan"
      (plan-sql
        "SELECT foo.name foo_name, bar.name bar_name
        FROM foo, bar
        WHERE foo.SYSTEM_TIME OVERLAPS bar.SYSTEM_TIME"
        {:table-info {"foo" #{"name"}
                      "bar" #{"name"}}}))))

(deftest test-valid-time-correlated-subquery
  (t/is (=plan-file
         "test-valid-time-correlated-subquery-where"
         (plan-sql "SELECT (SELECT foo.name
                    FROM foo
                    WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM bar"
                   {:table-info {"foo" #{"name"}
                                 "bar" #{}}})))

  (t/is (=plan-file
         "test-valid-time-correlated-subquery-projection"
         (plan-sql "SELECT (SELECT (foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM foo)
                    FROM bar"
                   {:table-info {"foo" #{"name"}
                                 "bar" #{}}}))))

(deftest test-derived-columns-with-periods
  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-predicate"
    (plan-sql
     "SELECT f.VALID_TIME OVERLAPS f.SYSTEM_TIME
        FROM foo
        AS f (_system_from, _system_to, _valid_from, _valid_to)"
     {:table-info {"foo" #{}}})))

  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-specs"
    (plan-sql
     "SELECT f.bar
        FROM foo
        FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
        FOR VALID_TIME AS OF CURRENT_TIMESTAMP
        AS f (bar)"
     {:table-info {"foo" #{"bar"}}}))))

(deftest test-for-all-valid-time-387
  (t/is (=plan-file
         "test-for-all-valid-time-387-query"
         (plan-sql "SELECT bar FROM foo FOR ALL VALID_TIME"
                   {:table-info {"foo" #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-query"
         (plan-sql "SELECT foo.bar FROM foo FOR VALID_TIME ALL"
                   {:table-info {"foo" #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-update"
         (plan-sql "UPDATE users FOR ALL VALID_TIME SET first_name = 'Sue'"
                   {:table-info {"users" #{}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-delete"
         (plan-sql "DELETE FROM users FOR ALL VALID_TIME"
                   {:table-info {"users" #{}}})))

  (t/is (=plan-file
         "test-for-all-valid-time-387-delete"
         (plan-sql "DELETE FROM users FOR VALID_TIME ALL"
                   {:table-info {"users" #{}}}))))

(deftest test-for-all-system-time-404
  (t/is (=plan-file
         "test-for-all-system-time-404"
         (plan-sql "SELECT bar FROM foo FOR ALL SYSTEM_TIME"
                   {:table-info {"foo" #{"bar"}}})))

  (t/is (=plan-file
         "test-for-all-system-time-404"
         (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME ALL"
                   {:table-info {"foo" #{"bar"}}}))))

(deftest test-period-specs-with-subqueries-407
  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-system-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR ALL SYSTEM_TIME) as tmp"
     {:table-info {"foo" #{"bar"}}})))

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-app-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR VALID_TIME AS OF NOW) as tmp"
     {:table-info {"foo" #{"bar"}}})))

  (t/is
   (=plan-file
    "test-period-specs-with-dml-subqueries-and-defaults-407" ;;also #424
    (plan-sql "INSERT INTO prop_owner (_id, customer_number, property_number, _valid_from, _valid_to)
                SELECT 1,
                145,
                7797, DATE '1998-01-03', tmp.app_start
                FROM
                (SELECT MIN(Prop_Owner._system_from) AS app_start
                FROM Prop_Owner
                FOR ALL SYSTEM_TIME
                WHERE Prop_Owner.id = 1) AS tmp"
              {:table-info {"prop_owner" #{"id"}}}))))

(deftest parenthesized-joined-tables-are-unboxed-502
  (t/is (= (plan-sql "SELECT 1 FROM ( tab0 JOIN tab2 ON TRUE )"
                     {:table-info {"tab0" #{}, "tab2" #{}}})
           (plan-sql "SELECT 1 FROM tab0 JOIN tab2 ON TRUE"
                     {:table-info {"tab0" #{}, "tab2" #{}}}))))

(deftest test-with-clause
  (t/is (=plan-file
          "test-with-clause"
          (plan-sql "WITH foo AS (SELECT id FROM bar WHERE id = 5)
                    SELECT foo.id foo_id, baz.id baz_id
                    FROM foo, foo AS baz"
                    {:table-info {"bar" #{"id"}}}))))

(deftest test-delimited-identifiers-in-insert-column-list-2549
  (t/is (=plan-file
          "test-delimited-identifiers-in-insert-column-list-2549"
          (plan-sql
            "INSERT INTO posts (_id, \"user-id\") VALUES (1234, 5678)"))))

(deftest test-table-period-specification-ordering-2260
  (let [opts {:table-info {"foo" #{"bar"}}}
        v-s (plan-sql
              "SELECT foo.bar
               FROM foo
                 FOR ALL VALID_TIME
                 FOR ALL SYSTEM_TIME"
              opts)
        s-v (plan-sql
              "SELECT foo.bar
               FROM foo
                 FOR ALL SYSTEM_TIME
                 FOR ALL VALID_TIME"
              opts)]

    (t/is (=plan-file "test-table-period-specification-ordering-2260-v-s" v-s))

    (t/is (=plan-file "test-table-period-specification-ordering-2260-s-v" s-v))

    (t/is (= v-s s-v))))

(deftest array-agg-decorrelation

  (t/testing "ARRAY_AGG is not decorrelated using rule-9 as array_agg(null) =/= array_agg(empty-rel)"

    (t/is (=plan-file
           "array-agg-decorrelation-1"
           (plan-sql "SELECT (SELECT sum(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0"
                     {:table-info {"tab0" #{"z"}}})))
    (t/is (=plan-file
           "array-agg-decorrelation-2"
           (plan-sql "SELECT (SELECT ARRAY_AGG(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0"
                     {:table-info {"tab0" #{"z"}}})))))

(deftest test-order-by-3065
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

(deftest test-order-by-unqualified-derived-column-refs
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

(deftest test-select-star-projections
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

  (t/is (= #{{:xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 2,
              :y "b",
              :xt/id 2}
             {:xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 3,
              :y "a",
              :xt/id 1}
             {:xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 1,
              :y "c",
              :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs._valid_from FROM docs"))))

  (t/is (= #{{:x 2,
              :y "b",
              :xt/id 2,
              :xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 3,
              :y "a",
              :xt/id 1,
              :xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 1,
              :y "c",
              :xt/id 3,
              :xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]"}}
             (set (xt/q tu/*node* "SELECT docs.*, docs._valid_from FROM docs WHERE docs._system_to = docs._valid_to")))))

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
             :xt/valid-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]"
             :xt/system-from #xt.time/zoned-date-time "2020-01-01T00:00Z[UTC]"}]
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

(deftest test-select-star-qualified-join
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

(deftest test-expand-asterisk-parenthesized-joined-table
  ;;Parens in the place of a table primary creates a parenthesized_joined_table
  ;;which is a qualified join, but nested inside a table primary. This test checks
  ;;that the expand asterisk attribute continues to traverse through the outer table
  ;;primary to find the nested table primaries.

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]
                           [:put-docs :bar {:xt/id 2 :a "one"}]])

  (t/is (= [{:foo-id 1 :bar-id 2 :a "one"}]

           (xt/q tu/*node*
                 "SELECT * RENAME (foo._id AS foo_id, bar._id AS bar_id) FROM ( foo LEFT JOIN bar ON true )"))))

(deftest test-select-star-lateral-join
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one"}]])

  (t/is (= [{:x-b "one", :outer-b "one", :z-b "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x (x_b), LATERAL (SELECT y.b AS z_b, x_b FROM y) AS z (z_b, outer_b)"))))

(deftest test-select-star-subquery
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one" :a 2}]])

  (t/is (= [{:b "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x"))))

(deftest test-sql-over-scanning
  (t/is
   (=plan-file
    "test-sql-over-scanning-col-ref"
    (plan-sql "SELECT foo.name FROM foo"
              {:table-info {"foo" #{"name" "lastname"}}}))

   "Tests only those columns required by the query are scanned for, rather than all those present on the base table")

  (t/is
   (=plan-file
    "test-sql-over-scanning-qualified-asterisk"
    (plan-sql "SELECT foo.*, bar.jame FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk"
    (plan-sql "SELECT * FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-subquery"
    (plan-sql "SELECT foo.*, (SELECT * FROM baz) FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}
                            "baz" #{"frame"}}})))
  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-from-subquery"
    (plan-sql "SELECT bar.* FROM (SELECT foo.a, foo.b FROM foo) AS bar"
              {:table-info {"foo" #{"a" "b"}}}))))

(deftest test-schema-qualified-names
  (t/is
   (=plan-file
    "test-schema-qualified-names-fully-qualified"
    (plan-sql "SELECT information_schema.columns.column_name FROM information_schema.columns")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-aliased-table"
    (plan-sql "SELECT f.column_name FROM information_schema.columns AS f")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-implict-pg_catalog"
    (plan-sql "SELECT pg_attribute.attname FROM pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-unqualified-col-ref"
    (plan-sql "SELECT pg_attribute.attname FROM pg_catalog.pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-qualified-col-ref"
    (plan-sql "SELECT pg_catalog.pg_attribute.attname FROM pg_attribute")))

  #_ ; FIXME field access
  (t/is
   (=plan-file
    "test-schema-qualified-names-field"
    (plan-sql "SELECT information_schema.columns.column_name.my_field FROM information_schema.columns")))

  ;;errors
  ;;
  #_ ; TODO these now return warnings which we should arguably test for
  (t/testing "Invalid Queries"
    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"PG_CATALOG.columns.column_name is an invalid reference to columns, schema name does not match"
      (plan-sql "SELECT pg_catalog.columns.column_name FROM information_schema.columns")))

    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"INFORMATION_SCHEMA.f.column_name is an invalid reference to f, schema name does not match"
      (plan-sql "SELECT information_schema.f.column_name FROM information_schema.columns AS f")))

    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"PG_CATALOG.f.column_name is an invalid reference to f, schema name does not match"
      (plan-sql "SELECT pg_catalog.f.column_name FROM information_schema.columns AS f")))))

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
          (plan-sql "SELECT _id AS order_id, value,
                            NEST_ONE(SELECT c.name FROM customers c WHERE c._id = o.customer_id) AS customer
                     FROM orders o"
                    {:table-info {"orders" #{"xt$id" "value" "customer_id"}
                                  "customers" #{"xt$id" "name"}}})))

  (t/is (=plan-file "test-nest-many"
          (plan-sql "SELECT c._id AS customer_id, c.name,
                            NEST_MANY(SELECT o._id AS order_id, o.value
                                      FROM orders o
                                      WHERE o.customer_id = c._id)
                              AS orders
                     FROM customers c"
                    {:table-info {"orders" #{"xt$id" "value" "customer_id"}
                                  "customers" #{"xt$id" "name"}}})))

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

(deftest test-invalid-xt-id-in-query-3324
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0 :name "bob"}]])
  (t/is (= [] (xt/q tu/*node* "SELECT foo.name FROM foo WHERE foo._id = NULL")))
  (t/is (= [] (xt/q tu/*node* "SELECT foo.name FROM foo WHERE foo._id = ?" {:args [nil]}))))

#_ ; TODO Add this?
(deftest test-random-fn
  (t/is (= true (-> (xt/q tu/*node* "SELECT 0.0 <= random() AS greater") first :greater)))
  (t/is (= true (-> (xt/q tu/*node* "SELECT random() < 1.0 AS smaller ") first :smaller))))

(t/deftest test-tx-ops-sql-params
  (t/testing "correct number of args"
    (t/is (= (serde/->tx-committed 0 #xt.time/instant "2020-01-01T00:00:00Z")
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)" [1 "dan"] [2 "james"]]])))

    (t/is (= [{:u-name "dan", :xt/id 1}
              {:u-name "james", :xt/id 2}]
             (xt/q tu/*node* "SELECT users._id, users.u_name FROM users ORDER BY _id"))))

  (t/testing "no arg rows provided when args expected"
    (t/is (= (serde/->tx-aborted 1
                                 #xt.time/instant "2020-01-02T00:00:00Z"
                                 #xt/runtime-err [:xtdb.indexer/missing-sql-args "Arguments list was expected but not provided" {:param-count 2}])
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)"]]))))

  (t/testing "incorrect number of args on all arg-row"
    (t/is (= (serde/->tx-aborted 2
                                 #xt.time/instant "2020-01-03T00:00:00Z"
                                 #xt/runtime-err [:xtdb.indexer/incorrect-sql-arg-count "1 arguments were provided and 2 arguments were provided" {:param-count 2, :arg-count 1}])
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)" [3] [4]]]))))

  (t/testing "incorrect number of args on one row"
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"All SQL arg-rows must have the same number of columns"
           (xt/submit-tx tu/*node* [[:sql "INSERT INTO users(_id, u_name) VALUES (?, ?)" [3 "finn"] [4]]])))))

(t/deftest test-case-sensitivity-in-delimited-cols
  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql "INSERT INTO T1(_id, col1, col2) VALUES(1,'fish',1000)"]])))
  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql "INSERT INTO t1(_id, COL1, COL2) VALUES(2,'dog',2000)"]])))

  (t/is (= [{:xt/id 2, :col1 "dog", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1._ID, T1.col1, t1.COL2 FROM T1")
           (xt/q tu/*node* "SELECT \"t1\"._id, T1.\"col1\", t1.COL2 FROM t1")))

  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql
                                                "INSERT INTO \"T1\"(_id, \"CoL1\", \"col2\") VALUES(3,'cat',3000)"]])))

  (t/is (= [{:xt/id 3, :CoL1 "cat", :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\"._id, \"T1\".\"CoL1\", \"T1\".COL2 FROM \"T1\"")))

  (t/is (= [{:xt/id 3, :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\"._id, \"T1\".col1, \"T1\".COL2 FROM \"T1\""))
        "can't refer to it as `col1`, have to quote")

  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql "UPDATE T1 SET Col1 = 'cat' WHERE t1.COL2 IN (313, 2000)"]])))

  (t/is (= [{:xt/id 2, :col1 "cat", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1._id, T1.col1, \"t1\".col2 FROM t1")))

  (t/is (= [{:col1 "cat", :avg 2000.0} {:col1 "fish", :avg 1000.0}]
           (xt/q tu/*node* "SELECT T1.col1, AVG(t1.col2) avg FROM t1 GROUP BY T1.col1")))
  
  (t/is (= [{:col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"TEEONE\".col2 FROM \"T1\" AS \"TEEONE\" WHERE \"TEEONE\".\"CoL1\" IN ( SELECT t1.\"col1\" FROM T1 WHERE T1.col1 = \"TEEONE\".\"CoL1\" ) ORDER BY \"TEEONE\".col2")))

  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql "DELETE FROM T1 WHERE t1.Col1 = 'fish'"]])))

  (t/is (= [{:xt/id 2}]
           (xt/q tu/*node* "SELECT t1._ID FROM T1")))

  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql
                                                "DELETE FROM \"T1\" WHERE \"T1\".\"col2\" IN (2000, 3000)"]])))

  (t/is (= [] (xt/q tu/*node* "SELECT \"T1\"._id FROM \"T1\""))))

(t/deftest test-ordering-of-intersect-and-union
  (xt/submit-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                           [:put-docs :t2 {:xt/id 1 :x 1}]
                           [:put-docs :t3 {:xt/id 1 :x 3}]])

  (t/is (= [{:x 1} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM t1 INTERSECT SELECT x FROM t2 UNION SELECT x FROM t3"))))

(deftest test-set-operations-with-different-column-names
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

(deftest test-union-all
  (xt/execute-tx tu/*node* [[:put-docs :foo1 {:xt/id 1 :x 1}]
                            [:put-docs :foo1 {:xt/id 2 :x 2}]
                            [:put-docs :foo2 {:xt/id 1 :x 1}]])

  (t/is (= [{:x 2} {:x 1}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION SELECT x FROM foo2")))

  (t/is (= [{:x 2} {:x 1} {:x 1}]
           (xt/q tu/*node* "SELECT x FROM foo1 UNION ALL SELECT x FROM foo2"))))

(deftest test-union-except
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

(deftest test-join-cond-subqueries
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, x) VALUES (1, 1), (2, 2)"]
                            [:sql "INSERT INTO bar (_id, x) VALUES (1, 1), (2, 3)"]
                            [:sql "INSERT INTO baz (_id, x) VALUES (1, 2)"]])
  (t/is (= [{:x 2, :bar-x 3} {:x 2, :bar-x 1} {:x 1}] 
           (xt/q tu/*node* "SELECT foo.x, bar.x bar_x FROM foo LEFT JOIN bar ON foo.x = (SELECT baz.x FROM baz)")))
  
  (t/is (= [{:x 2}, {:x 1}]
           (xt/q tu/*node* "SELECT foo.x, bar.x bar_x FROM foo LEFT JOIN bar ON bar.x = (SELECT baz.x FROM baz WHERE baz.x = foo.x)"))))

(deftest test-erase-with-subquery
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :foo :bar 1}]])
  (t/is (:committed? (xt/execute-tx tu/*node* [[:sql "ERASE FROM docs WHERE docs._id IN (SELECT docs._id FROM docs WHERE docs.bar = 1)"]])))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM docs"))))

(deftest select-star-on-non-existent-tables-3005
  (t/is (= [] (xt/q tu/*node* "select users.first_name from users")))
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM users"))))

(deftest nested-parenthesized-joined-tables-3185
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

(deftest test-delete-with-unqualified-col-3017
  (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id 1 :col "val"}]])

  (t/is (= [{:xt/id 1, :col "val"}]
           (xt/q tu/*node* "SELECT * FROM table")))

  (xt/submit-tx tu/*node* [[:sql "DELETE FROM table WHERE col = 'val'"]])

  (t/is (empty? (xt/q tu/*node* "SELECT * FROM table"))))

(deftest test-c-style-escapes-3415
  (t/is (= ["A" "A" "A" "" "%" "😁" "\n" "'"]
           (->> (xt/q tu/*node* "VALUES E'\\x41', E'\\101', E'\\U00000041', E'', E'%', E'\\U0001F601', E'\\n', E'\\''")
                (mapv :xt/column-1)))))

(deftest forbid-updating-core-cols-3433
  (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id 1}]])

  (letfn [(f [sql]
            (throw (:error (xt/execute-tx tu/*node* [[:sql sql]]))))]

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Cannot UPDATE id column"
                            (f "UPDATE table SET _id = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Cannot UPDATE valid_from column"
                            (f "UPDATE table SET _valid_from = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Cannot UPDATE valid_to column"
                            (f "UPDATE table SET _valid_to = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Cannot UPDATE system_from column"
                            (f "UPDATE table SET _system_from = DATE '2024-01-01' WHERE _id = 1")))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Cannot UPDATE system_to column"
                            (f "UPDATE table SET _system_to = DATE '2024-01-01' WHERE _id = 1")))))

(deftest disallow-period-specs-on-ctes-3440
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Period specifications not allowed on CTE reference: my_cte"
                          (xt/q tu/*node* "WITH my_cte AS (SELECT * FROM docs) SELECT * FROM my_cte FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01'")))

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Period specifications not allowed on CTE reference: my_cte"
                          (xt/q tu/*node* "WITH my_cte AS (SELECT * FROM docs) SELECT * FROM my_cte FOR VALID_TIME AS OF TIMESTAMP '2024-01-01'"))))

(deftest test-assert-3445
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 1}]])
  (t/is (true? (:committed? (xt/execute-tx tu/*node* [[:sql "ASSERT (SELECT COUNT(*) FROM docs) = 1"]
                                                      [:put-docs :docs {:xt/id 2 :x 2}]]))))

  (let [{:keys [committed? error]} (xt/execute-tx tu/*node* [[:sql "ASSERT (SELECT COUNT(*) FROM docs) = 1"]
                                                             [:put-docs :docs {:xt/id 3 :x 3}]])]
    (t/is (false? committed?))
    (t/is (= :xtdb/assert-failed (:xtdb.error/error-key (ex-data error)))))

  (t/is (= [{:doc-count 2}] (xt/q tu/*node* "SELECT COUNT(*) doc_count FROM docs"))))

(deftest test-date-id-caught-3446
  (let [{:keys [committed? error]} (xt/execute-tx tu/*node* [[:sql "INSERT into readings (_id, value) VALUES (DATE '2020-01-06', 4)"]])]
    (t/is (false? committed?))
    (t/is (thrown-with-msg? RuntimeException
                            #"Invalid ID type: java.time.LocalDate"
                            (throw error)))

    (t/testing "node continues"
      (xt/execute-tx tu/*node* [[:put-docs :readings {:xt/id 1 :value 4}]])

      (t/is (= [{:xt/id 1, :value 4}]
               (xt/q tu/*node* "SELECT * FROM readings"))))))

(deftest test-disallow-col-refs-in-period-specs-3447
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"line 1:41 mismatched input 'foo' expecting"
                          (xt/q tu/*node* "SELECT * FROM docs FOR SYSTEM_TIME AS OF foo")))

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"line 1:42 mismatched input 'bar' expecting"
                          (xt/q tu/*node* "SELECT * FROM docs FOR VALID_TIME BETWEEN bar AND baz"))))

(deftest test-portion-of-valid-time-boundary
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO system_active_power (_id, value, _valid_from, _valid_to)
VALUES
(1, 500,  TIMESTAMP '2024-01-01T00:00:00', TIMESTAMP '2024-01-01T00:05:00'),
(1, 510,  TIMESTAMP '2024-01-01T00:05:00', TIMESTAMP '2024-01-01T00:10:00'),
(1, 530,  TIMESTAMP '2024-01-01T00:10:00', TIMESTAMP '2024-01-01T00:15:00'),
(1, 9999, TIMESTAMP '2024-01-01T00:15:00', TIMESTAMP '2024-01-01T00:20:00'),
(1, 560,  TIMESTAMP '2024-01-01T00:20:00', TIMESTAMP '2024-01-01T00:25:00'),
(1, 580,  TIMESTAMP '2024-01-01T00:25:00', TIMESTAMP '2024-01-01T00:30:00')"]])

  (letfn [(zdt [mins]
            (.plusMinutes #xt.time/zoned-date-time "2024-01-01T00:00Z[UTC]" mins))
          (q []
            (set (xt/q tu/*node* "SELECT value, _valid_from, _valid_to
                                  FROM system_active_power FOR ALL VALID_TIME WHERE _id = 1")))
          (del! [from to]
            (xt/execute-tx tu/*node* [[:sql "DELETE FROM system_active_power
                                                          FOR PORTION OF VALID_TIME FROM ? TO ?
                                                          WHERE _id = 1"
                                       [from to]]]))]

    (t/is (true? (:committed? (del! (zdt 15) (zdt 20)))))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 5)}
               {:value 510, :xt/valid-from (zdt 5), :xt/valid-to (zdt 10)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 560, :xt/valid-from (zdt 20), :xt/valid-to (zdt 25)}
               {:value 580, :xt/valid-from (zdt 25), :xt/valid-to (zdt 30)}}
             (q)))

    (t/is (true? (:committed? (del! (zdt 2) (zdt 10)))))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 2)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 560, :xt/valid-from (zdt 20), :xt/valid-to (zdt 25)}
               {:value 580, :xt/valid-from (zdt 25), :xt/valid-to (zdt 30)}}
             (q)))

    (t/is (true? (:committed? (del! (zdt 20) (zdt 27)))))

    (t/is (= #{{:value 500, :xt/valid-from (zdt 0), :xt/valid-to (zdt 2)}
               {:value 530, :xt/valid-from (zdt 10), :xt/valid-to (zdt 15)}
               {:value 580, :xt/valid-from (zdt 27), :xt/valid-to (zdt 30)}}
             (q)))))
