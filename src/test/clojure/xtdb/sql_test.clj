(ns xtdb.sql-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.sql.analyze :as sem]
            [xtdb.logical-plan :as lp]
            [xtdb.sql :as sql])

  (:import (java.time LocalDateTime)))

(defn plan-sql
  ([sql opts] (sql/compile-query sql (into {:default-all-valid-time? true} opts)))
  ([sql] (plan-sql sql {:decorrelate? true, :validate-plan? true, :instrument-rules? true})))

(def regen-expected-files? false)

(defmethod t/assert-expr '=plan-file [msg form]
  `(let [exp-plan-file-name# ~(nth form 1)
         exp-plan-file-path# (format "xtdb/sql/plan_test_expectations/%s.edn" exp-plan-file-name#)
         actual-plan# ~(nth form 2)]
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
            :expected exp-plan-file-path#  :actual (Exception. "Missing Expectation File")})))))

(deftest test-basic-queries
  (t/is (=plan-file
          "basic-query-1"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960")))

  (t/is (=plan-file
          "basic-query-2"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950")))

  (t/is (=plan-file
          "basic-query-3"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'")))

  (t/is (=plan-file
          "basic-query-4"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name")))

  (t/is (=plan-file
          "basic-query-5"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")))

  (t/is (=plan-file
          "basic-query-6"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")))

  (t/is (=plan-file
          "basic-query-7"
          (plan-sql "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)")))

  (t/is (=plan-file
          "basic-query-8"
          (plan-sql "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)")))

  (t/is (=plan-file
          "basic-query-9"
          (plan-sql "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930")))

  (t/is (=plan-file
          "basic-query-10"
          (plan-sql "SELECT SUM(m.length) FROM Movie AS m")))

  (t/is (=plan-file
          "basic-query-11"
          (plan-sql "SELECT * FROM StarsIn AS si(name)")))

  (t/is (=plan-file
          "basic-query-12"
          (plan-sql "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)")))

  (t/is (=plan-file
          "basic-query-13"
           (plan-sql "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname")))

  (t/is (=plan-file
          "basic-query-14"
           (plan-sql "SELECT DISTINCT si.movieTitle FROM StarsIn AS si")))

  (t/is (=plan-file
          "basic-query-15"
          (plan-sql "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si")))

  (t/is (=plan-file
          "basic-query-16"
           (plan-sql "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si")))

  (t/is (=plan-file
          "basic-query-17"
          (plan-sql "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si")))

  (t/is (=plan-file
          "basic-query-18"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si")))

  (t/is (=plan-file
          "basic-query-19"
          (plan-sql "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name")))

  (t/is (=plan-file
          "basic-query-20"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY")))

  (t/is (=plan-file
          "basic-query-21"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS")))

  (t/is (=plan-file
          "basic-query-22"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 LIMIT 10")))

  (t/is (=plan-file
          "basic-query-23"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle")))

  (t/is (=plan-file
          "basic-query-24"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS")))

  (t/is (=plan-file
          "basic-query-25"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC")))

  (t/is (=plan-file
          "basic-query-26"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle")))

  (t/is (=plan-file
          "basic-query-27"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year")))

  (t/is (=plan-file
          "basic-query-28"
           (plan-sql "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'")))

  (t/is (=plan-file
          "basic-query-29"
          (plan-sql "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)")))

  (t/is (=plan-file
          "basic-query-30"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film")))

  (t/is (=plan-file
          "basic-query-31"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film")))

  (t/is (=plan-file
          "basic-query-32"
          (plan-sql "VALUES (1, 2), (3, 4)")))

  (t/is (=plan-file
          "basic-query-33"
           (plan-sql "VALUES 1, 2")))

  (t/is (=plan-file
          "basic-query-34"
          (plan-sql "SELECT CASE t1.a + 1 WHEN t1.b THEN 111 WHEN t1.c THEN 222 WHEN t1.d THEN 333 WHEN t1.e THEN 444 ELSE 555 END,
                    CASE WHEN t1.a < t1.b - 3 THEN 111 WHEN t1.a <= t1.b THEN 222 WHEN t1.a < t1.b+3 THEN 333 ELSE 444 END,
                    CASE t1.a + 1 WHEN t1.b, t1.c THEN 222 WHEN t1.d, t1.e + 1 THEN 444 ELSE 555 END FROM t1")))

  (t/is (=plan-file
          "basic-query-35"
          (plan-sql "SELECT * FROM t1 WHERE t1.a IS NULL")))

  (t/is (=plan-file
          "basic-query-36"
          (plan-sql "SELECT * FROM t1 WHERE t1.a IS NOT NULL")))

  (t/is (=plan-file
          "basic-query-37"
          (plan-sql "SELECT NULLIF(t1.a, t1.b) FROM t1"))))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (=plan-file
            "scalar-subquery-in-select"
            (plan-sql "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"))))

  (t/testing "Scalar subquery in WHERE"
    (t/is (=plan-file
            "scalar-subquery-in-where"
            (plan-sql "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"))))

  (t/testing "Correlated scalar subquery in SELECT"
    (t/is (=plan-file
            "correlated-scalar-subquery-in-select"
            (plan-sql "SELECT (1 = (SELECT foo.bar = x.y FROM foo)) AS some_column FROM x WHERE x.y = 1"))))

  (t/testing "EXISTS in WHERE"
    (t/is (=plan-file
            "exists-in-where"
             (plan-sql "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10.0"))))

  (t/testing "EXISTS as expression in SELECT"
    (t/is (=plan-file
            "exists-as-expression-in-select"
            (plan-sql "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"))))

  (t/testing "NOT EXISTS in WHERE"
    (t/is (=plan-file
            "not-exists-in-where"
            (plan-sql "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"))))

  (t/testing "IN in WHERE"
    (t/is (=plan-file
            "in-in-where-select"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)")))

    (t/is (=plan-file
            "in-in-where-set"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (1, 2)"))))

  (t/testing "NOT IN in WHERE"
    (t/is (=plan-file
            "not-in-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"))))

  (t/testing "ALL in WHERE"
    (t/is (=plan-file
            "all-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"))))

  (t/testing "ANY in WHERE"
    (t/is (=plan-file
            "any-in-where"
            (plan-sql "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"))))

  (t/testing "ALL as expression in SELECT"
    (t/is (=plan-file
            "all-as-expression-in-select"
            (plan-sql "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"))))

  (t/testing "LATERAL derived table"
    (t/is (=plan-file
            "lateral-derived-table-1"
            (plan-sql "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y")))

    (t/is (=plan-file
            "lateral-derived-table-2"
            (plan-sql "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"))))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (t/is (=plan-file
            "decorrelation-1"
            (plan-sql "SELECT c.custkey FROM customer c
                      WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)")))

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (t/is (=plan-file
            "decorrelation-2"
            (plan-sql "SELECT * FROM customers
                      WHERE customers.country = 'Mexico' AND
                      EXISTS (SELECT * FROM orders WHERE customers.custno = orders.custno)")))

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (t/is (=plan-file
            "decorrelation-3"
            (plan-sql "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
                      FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)")))

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (t/is (=plan-file
            "decorrelation-4"
            (plan-sql "SELECT s.name, e.course
                      FROM students s, exams e
                      WHERE s.id = e.sid AND
                      e.grade = (SELECT MIN(e2.grade)
                      FROM exams e2
                      WHERE s.id = e2.sid)")))

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
              s.year > e2.date))")))


    (t/testing "Subqueries in join conditions"

      (->> "uncorrelated subquery"
           (t/is (=plan-file
                   "subquery-in-join-uncorrelated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo)"))))

      (->> "correlated subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"))))

      ;; TODO unable to decorr, need to be able to pull the select over the max-1-row
      ;; although should be able to do this now, no such thing as max-1-row any more
      (->> "correlated equalty subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-equality-subquery"
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)")))))))

(t/deftest parameters-referenced-in-relation-test
  (t/are [expected plan apply-columns]
         (= expected (lp/parameters-referenced-in-relation? plan (vals apply-columns)))
         true '[:table [{x6 ?x8}]] '{x2 ?x8}
         false '[:table [{x6 ?x4}]] '{x2 ?x8}))


(deftest non-semi-join-subquery-optimizations-test
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-1"
          (plan-sql "select f.a from foo f where f.a in (1,2) or f.b = 42"))
        "should not be decorrelated")
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-2"
          (plan-sql "select f.a from foo f where true = (EXISTS (SELECT foo.c from foo))"))
        "should be decorrelated as a cross join, not a semi/anti join"))

(deftest multiple-ins-in-where-clause
  (t/is (=plan-file
          "multiple-ins-in-where-clause"
          (plan-sql "select f.a from foo f where f.a in (1,2) AND f.a = 42 AND f.b in (3,4)"))))

(deftest deeply-nested-correlated-query ;;TODO broken
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
                    WHERE R3.A = R2.B AND R3.B = S.C))"))))

(t/deftest test-array-element-reference-107
  (t/is (=plan-file
          "test-array-element-reference-107-1"
          (plan-sql "SELECT u.a[1] AS first_el FROM u")))

  (t/is (=plan-file
          "test-array-element-reference-107-2"
          (plan-sql "SELECT u.b[u.a[1]] AS dyn_idx FROM u"))))

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
                    FROM u"))))

(t/deftest test-dynamic-parameters-103
  (t/is (=plan-file
         "test-dynamic-parameters-103-1"
         (plan-sql "SELECT foo.a FROM foo WHERE foo.b = ? AND foo.c = ?")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-2"
         (plan-sql "SELECT foo.a
                    FROM foo, (SELECT bar.b FROM bar WHERE bar.c = ?) bar (b)
                    WHERE foo.b = ? AND foo.c = ?")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-subquery-project"
         (plan-sql "SELECT t1.col1, (SELECT ? FROM bar WHERE bar.col1 = 4) FROM t1")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-top-level-project"
         (plan-sql "SELECT t1.col1, ? FROM t1")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-set-value"
         (plan-sql "UPDATE t1 SET col1 = ?")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-table-values"
         (plan-sql "SELECT bar.foo FROM (VALUES (?)) AS bar(foo)")))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-app-time"
         (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"))))

(t/deftest test-order-by-null-handling-159
  (t/is (=plan-file
         "test-order-by-null-handling-159-1"
         (plan-sql "SELECT foo.a FROM foo ORDER BY foo.a NULLS FIRST")))

  (t/is (=plan-file
         "test-order-by-null-handling-159-2"
         (plan-sql "SELECT foo.a FROM foo ORDER BY foo.a NULLS LAST"))))

(t/deftest test-arrow-table
  (t/is (=plan-file
          "test-arrow-table-1"
          (plan-sql "SELECT foo.a FROM ARROW_TABLE('test.arrow') AS foo")))

  (t/is (=plan-file
          "test-arrow-table-2"
          (plan-sql "SELECT * FROM ARROW_TABLE('test.arrow') AS foo (a, b)"))))

(defn- plan-expr [sql]
  (let [plan (plan-sql (format "SELECT %s t FROM foo WHERE foo.a = 42" sql))
        expr (some (fn [form]
                       (when (and (vector? form) (= :project (first form)))
                         (let [[_ projections] form]
                           (val (ffirst projections)))))
                     (tree-seq seqable? seq plan))]
    expr))

(t/deftest test-trim-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "TRIM(foo.a)" '(trim x1 " ")

    "TRIM(LEADING FROM foo.a)" '(trim-leading x1 " ")
    "TRIM(LEADING '$' FROM foo.a)" '(trim-leading x1 "$")
    "TRIM(LEADING foo.b FROM foo.a)" '(trim-leading x2 x1)

    "TRIM(TRAILING FROM foo.a)" '(trim-trailing x1 " ")
    "TRIM(TRAILING '$' FROM foo.a)" '(trim-trailing x1 "$")
    "TRIM(TRAILING foo.b FROM foo.a)" '(trim-trailing x2 x1)

    "TRIM(BOTH FROM foo.a)" '(trim x1 " ")
    "TRIM(BOTH '$' FROM foo.a)" '(trim x1 "$")
    "TRIM(BOTH foo.b FROM foo.a)" '(trim x2 x1)

    "TRIM(BOTH '😎' FROM foo.a)" '(trim x1 "😎")

    "TRIM('$' FROM foo.a)" '(trim x1 "$")))

(t/deftest test-like-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "foo.a LIKE ''" '(like x1 "")
    "foo.a LIKE foo.b" '(like x1 x2)
    "foo.a LIKE 'foo%'" '(like x1 "foo%")

    "foo.a NOT LIKE ''" '(not (like x1 ""))
    "foo.a NOT LIKE foo.b" '(not (like x1 x2))
    "foo.a NOT LIKE 'foo%'" '(not (like x1 "foo%"))

    ;; no support for ESCAPE (or default escapes), see #157
    ))

(t/deftest test-like-regex-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "foo.a LIKE_REGEX foo.b" '(like-regex x1 x2 "")
    "foo.a LIKE_REGEX foo.b FLAG 'i'" '(like-regex x1 x2 "i")

    "foo.a NOT LIKE_REGEX foo.b" '(not (like-regex x1 x2 ""))
    "foo.a NOT LIKE_REGEX foo.b FLAG 'i'" '(not (like-regex x1 x2 "i"))))

(t/deftest test-upper-expr
  (t/is (= '(upper x1) (plan-expr "UPPER(foo.a)"))))

(t/deftest test-lower-expr
  (t/is (= '(lower x1) (plan-expr "LOWER(foo.a)"))))

(t/deftest test-concat-expr
  (t/is (= '(concat x1 x2) (plan-expr "foo.a || foo.b")))
  (t/is (= '(concat "a" x1) (plan-expr "'a' || foo.b")))
  (t/is (= '(concat (concat x1 "a") "b") (plan-expr "foo.a || 'a' || 'b'"))))

(t/deftest test-character-length-expr
  (t/is (= '(character-length x1) (plan-expr "CHARACTER_LENGTH(foo.a)")))
  (t/is (= '(character-length x1) (plan-expr "CHARACTER_LENGTH(foo.a USING CHARACTERS)")))
  (t/is (= '(octet-length x1) (plan-expr "CHARACTER_LENGTH(foo.a USING OCTETS)"))))

(t/deftest test-char-length-alias
  (t/is (= '(character-length x1) (plan-expr "CHAR_LENGTH(foo.a)")) "CHAR_LENGTH alias works")
  (t/is (= '(character-length x1) (plan-expr "CHAR_LENGTH(foo.a USING CHARACTERS)")) "CHAR_LENGTH alias works")
  (t/is (= '(octet-length x1) (plan-expr "CHAR_LENGTH(foo.a USING OCTETS)")) "CHAR_LENGTH alias works"))

(t/deftest test-octet-length-expr
  (t/is (= '(octet-length x1) (plan-expr "OCTET_LENGTH(foo.a)"))))

(t/deftest test-position-expr
  (t/is (= '(position x1 x2) (plan-expr "POSITION(foo.a IN foo.b)")))
  (t/is (= '(position x1 x2) (plan-expr "POSITION(foo.a IN foo.b USING CHARACTERS)")))
  (t/is (= '(octet-position x1 x2) (plan-expr "POSITION(foo.a IN foo.b USING OCTETS)"))))

(t/deftest test-overlay-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))
    "OVERLAY(foo.a PLACING foo.b FROM 1 for 4)" '(overlay x1 x2 1 4)
    "OVERLAY(foo.a PLACING foo.b FROM 1)" '(overlay x1 x2 1 (default-overlay-length x2))))

(t/deftest test-bool-test-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "foo.a IS true" '(true? x1)
    "foo.a IS NOT true" '(not (true? x1))

    "foo.a IS false" '(false? x1)
    "foo.a IS NOT false" '(not (false? x1))

    "foo.a IS UNKNOWN" '(nil? x1)
    "foo.a IS NOT UNKNOWN" '(not (nil? x1))

    "foo.a IS NULL" '(nil? x1)
    "foo.a IS NOT NULL" '(not (nil? x1))))

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
            FROM orders) AS o"))))

(deftest test-semi-and-anti-joins-are-pushed-down
  (t/is (=plan-file
          "test-semi-and-anti-joins-are-pushed-down"
          (plan-sql
            "SELECT t1.a1
            FROM t1, t2, t3
            WHERE t1.b1 in (532,593)
            AND t2.b1 in (808,662)
            AND t3.c1 in (792,14)
            AND t1.a1 = t2.a2"))))

(deftest test-group-by-with-projected-column-in-expr
  (t/is (=plan-file
          "test-group-by-with-projected-column-in-expr"
          (plan-sql
            "SELECT foo.a - 4 AS bar
            FROM foo
            GROUP BY foo.a"))))

(deftest test-interval-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "1 YEAR" '(single-field-interval 1 "YEAR" 2 0)
    "1 YEAR + 3 MONTH + 4 DAY" '(+ (+ (single-field-interval 1 "YEAR" 2 0)
                                      (single-field-interval 3 "MONTH" 2 0))
                                   (single-field-interval 4 "DAY" 2 0))

    ;; todo investigate, these expr are entirely ambiguous
    ;; I think these should not be parsed, but they are! ...
    #_#_ "1 YEAR + 3" '()
    #_#_ "1 YEAR - 3" '()

    ;; scaling is not ambiguous like add/sub is
    "1 YEAR * 3" '(* (single-field-interval 1 "YEAR" 2 0) 3)
    "3 * 1 YEAR" '(* 3 (single-field-interval 1 "YEAR" 2 0))

    ;; division is allowed in spec, but provides some ambiguity
    ;; as we do not allow fractional components (other than seconds)
    ;; we therefore throw at runtime for arrow vectors that cannot be cleanly truncated / rond
    "1 YEAR / 3" '(/ (single-field-interval 1 "YEAR" 2 0) 3)

    "foo.a YEAR" '(single-field-interval x1 "YEAR" 2 0)
    "foo.a MONTH" '(single-field-interval x1 "MONTH" 2 0)
    "foo.a DAY" '(single-field-interval x1 "DAY" 2 0)
    "foo.a HOUR" '(single-field-interval x1 "HOUR" 2 0)
    "foo.a MINUTE" '(single-field-interval x1 "MINUTE" 2 0)
    "foo.a SECOND" '(single-field-interval x1 "SECOND" 2 6)

    "- foo.a SECOND" '(- (single-field-interval x1 "SECOND" 2 6))
    "+ foo.a SECOND" '(single-field-interval x1 "SECOND" 2 6)

    "foo.a YEAR + foo.b YEAR" '(+ (single-field-interval x1 "YEAR" 2 0)
                                  (single-field-interval x2 "YEAR" 2 0))
    "foo.a YEAR + foo.b MONTH" '(+ (single-field-interval x1 "YEAR" 2 0)
                                   (single-field-interval x2 "MONTH" 2 0))
    "foo.a YEAR - foo.b MONTH" '(- (single-field-interval x1 "YEAR" 2 0)
                                   (single-field-interval x2 "MONTH" 2 0))

    "foo.a YEAR + 1 MONTH" '(+ (single-field-interval x1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
    "foo.a YEAR + 1 MONTH + 2 DAY" '(+ (+ (single-field-interval x1 "YEAR" 2 0)
                                          (single-field-interval 1 "MONTH" 2 0))
                                       (single-field-interval 2 "DAY" 2 0))
    "foo.a YEAR + 1 MONTH - 2 DAY" '(- (+ (single-field-interval x1 "YEAR" 2 0)
                                          (single-field-interval 1 "MONTH" 2 0))
                                       (single-field-interval 2 "DAY" 2 0))

    "foo.a + 2 MONTH" '(+ x1 (single-field-interval 2 "MONTH" 2 0))
    "foo.a + +1 MONTH" '(+ x1 (single-field-interval 1 "MONTH" 2 0))
    "foo.a + -1 MONTH" '(+ x1 (- (single-field-interval 1 "MONTH" 2 0)))

    "foo.a YEAR TO MONTH" '(multi-field-interval x1 "YEAR" 2 "MONTH" 2)
    "foo.a DAY TO SECOND" '(multi-field-interval x1 "DAY" 2 "SECOND" 6)

    "INTERVAL '3' YEAR" '(single-field-interval "3" "YEAR" 2 0)
    "INTERVAL '-3' YEAR" '(single-field-interval "-3" "YEAR" 2 0)
    "INTERVAL '+3' YEAR" '(single-field-interval "+3" "YEAR" 2 0)
    "INTERVAL '333' YEAR(3)" '(single-field-interval "333" "YEAR" 3 0)

    "INTERVAL '3' MONTH" '(single-field-interval "3" "MONTH" 2 0)
    "INTERVAL '-3' MONTH" '(single-field-interval "-3" "MONTH" 2 0)
    "INTERVAL '+3' MONTH" '(single-field-interval "+3" "MONTH" 2 0)
    "INTERVAL '333' MONTH(3)" '(single-field-interval "333" "MONTH" 3 0)

    "INTERVAL '3' DAY" '(single-field-interval "3" "DAY" 2 0)
    "INTERVAL '-3' DAY" '(single-field-interval "-3" "DAY" 2 0)
    "INTERVAL '+3' DAY" '(single-field-interval "+3" "DAY" 2 0)
    "INTERVAL '333' DAY(3)" '(single-field-interval "333" "DAY" 3 0)

    "INTERVAL '3' HOUR" '(single-field-interval "3" "HOUR" 2 0)
    "INTERVAL '-3' HOUR" '(single-field-interval "-3" "HOUR" 2 0)
    "INTERVAL '+3' HOUR" '(single-field-interval "+3" "HOUR" 2 0)
    "INTERVAL '333' HOUR(3)" '(single-field-interval "333" "HOUR" 3 0)

    "INTERVAL '3' MINUTE" '(single-field-interval "3" "MINUTE" 2 0)
    "INTERVAL '-3' MINUTE" '(single-field-interval "-3" "MINUTE" 2 0)
    "INTERVAL '+3' MINUTE" '(single-field-interval "+3" "MINUTE" 2 0)
    "INTERVAL '333' MINUTE(3)" '(single-field-interval "333" "MINUTE" 3 0)

    "INTERVAL '3' SECOND" '(single-field-interval "3" "SECOND" 2 6)
    "INTERVAL '-3' SECOND" '(single-field-interval "-3" "SECOND" 2 6)
    "INTERVAL '+3' SECOND" '(single-field-interval "+3" "SECOND" 2 6)
    "INTERVAL '333' SECOND(3)" '(single-field-interval "333" "SECOND" 3 6)
    "INTERVAL '333.22' SECOND(3, 2)" '(single-field-interval "333.22" "SECOND" 3 2)

    "INTERVAL '3-4' YEAR TO MONTH" '(multi-field-interval "3-4" "YEAR" 2 "MONTH" 2)
    "INTERVAL '3-4' YEAR(3) TO MONTH" '(multi-field-interval "3-4" "YEAR" 3 "MONTH" 2)

    "INTERVAL '-3-4' YEAR TO MONTH" '(multi-field-interval "-3-4" "YEAR" 2 "MONTH" 2)
    "INTERVAL '+3-4' YEAR TO MONTH" '(multi-field-interval "+3-4" "YEAR" 2 "MONTH" 2)

    "INTERVAL '3 4' DAY TO HOUR" '(multi-field-interval "3 4" "DAY" 2 "HOUR" 2)
    "INTERVAL '3 04' DAY TO HOUR" '(multi-field-interval "3 04" "DAY" 2 "HOUR" 2)
    "INTERVAL '3 04:20' DAY TO MINUTE" '(multi-field-interval "3 04:20" "DAY" 2 "MINUTE" 2)
    "INTERVAL '3 04:20:34' DAY TO SECOND" '(multi-field-interval "3 04:20:34" "DAY" 2 "SECOND" 6)
    "INTERVAL '3 04:20:34' DAY(3) TO SECOND(4)" '(multi-field-interval "3 04:20:34" "DAY" 3 "SECOND" 4)
    "INTERVAL '3 04:20:34' DAY TO SECOND(4)" '(multi-field-interval "3 04:20:34" "DAY" 2 "SECOND" 4)

    "INTERVAL '04:20' HOUR TO MINUTE" '(multi-field-interval "04:20" "HOUR" 2 "MINUTE" 2)
    "INTERVAL '04:20:34' HOUR TO SECOND" '(multi-field-interval "04:20:34" "HOUR" 2 "SECOND" 6)

    "INTERVAL '20:34' MINUTE TO SECOND" '(multi-field-interval "20:34" "MINUTE" 2 "SECOND" 6)))

(deftest test-interval-abs
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "ABS(foo.a)" '(abs x1)
    "ABS(1 YEAR)" '(abs (single-field-interval 1 "YEAR" 2 0))))

(deftest test-array-construction
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "ARRAY []" []

    "ARRAY [1]" [1]
    "ARRAY [NULL]" [nil]
    "ARRAY [ARRAY [1]]" [[1]]

    "ARRAY [foo.x, foo.y + 1]" '[x1 (+ x2 1)]

    "ARRAY [1, 42]" [1 42]
    "ARRAY [1, NULL]" [1 nil]
    "ARRAY [1, 1.2, '42!']" [1 1.2 "42!"]

    "[]" []

    "[1]" [1]
    "[NULL]" [nil]
    "[[1]]" [[1]]

    "[foo.x, foo.y + 1]" '[x1 (+ x2 1)]

    "[1, 42]" [1 42]
    "[1, NULL]" [1 nil]
    "[1, 1.2, '42!']" [1 1.2 "42!"]))

(deftest test-object-construction
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "OBJECT ()" {}
    "OBJECT ('foo': 2)" {:foo 2}
    "OBJECT ('foo': 2, 'bar': true)" {:foo 2 :bar true}
    "OBJECT ('foo': 2, 'bar': ARRAY [true, 1])" {:foo 2 :bar [true 1]}
    "OBJECT ('foo': 2, 'bar': OBJECT('baz': ARRAY [true, 1]))" {:foo 2 :bar {:baz [true 1]}}

    "{}" {}
    "{'foo': 2}" {:foo 2}
    "{'foo': 2, 'bar': true}" {:foo 2 :bar true}
    "{'foo': 2, 'bar': [true, 1]}" {:foo 2 :bar [true 1]}
    "{'foo': 2, 'bar': {'baz': [true, 1]}}" {:foo 2 :bar {:baz [true 1]}}))

(deftest test-object-field-access
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "OBJECT('foo': 2).foo" '(. {:foo 2} :foo)
    "{'foo': 2}.foo" '(. {:foo 2} :foo)
    "{'foo': 2}.foo.bar" '(. (. {:foo 2} :foo) :bar)

    "foo.a.b" '(. x1 :b)
    "foo.a.b.c" '(. (. x1 :b) :c)))

(deftest test-array-subqueries
  (t/are [file q]
    (=plan-file file (plan-sql q))

    "test-array-subquery1" "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42"
    "test-array-subquery2" "SELECT ARRAY(select b.b1 from b where b.b2 = a.b) FROM a where a.a = 42"))

(t/deftest test-array-trim
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "TRIM_ARRAY(NULL, 2)" '(trim-array nil 2)
    "TRIM_ARRAY(foo.a, 2)" '(trim-array x1 2)
    "TRIM_ARRAY(ARRAY [42, 43], 1)" '(trim-array [42, 43] 1)
    "TRIM_ARRAY(foo.a, foo.b)" '(trim-array x1 x2)))

(t/deftest test-cast
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "CAST(NULL AS INT)" (list 'cast nil :i32)
    "CAST(NULL AS INTEGER)" (list 'cast nil :i32)
    "CAST(NULL AS BIGINT)" (list 'cast nil :i64)
    "CAST(NULL AS SMALLINT)" (list 'cast nil :i16)
    "CAST(NULL AS FLOAT)" (list 'cast nil :f32)
    "CAST(NULL AS REAL)" (list 'cast nil :f32)
    "CAST(NULL AS DOUBLE PRECISION)" (list 'cast nil :f64)

    "CAST(foo.a AS INT)" (list 'cast 'x1 :i32)
    "CAST(42.0 AS INT)" (list 'cast 42.0 :i32)))

(t/deftest test-expr-in-equi-join
  (t/is
    (=plan-file
      "test-expr-in-equi-join-1"
      (plan-sql "SELECT a.a FROM a JOIN bar b ON a.a+1 = b.b+1")))
  (t/is
    (=plan-file
      "test-expr-in-equi-join-2"
      (plan-sql "SELECT a.a FROM a JOIN bar b ON a.a = b.b+1"))))

(deftest push-semi-and-anti-joins-down-test
;;semi-join was previously been pushed down below the cross join
;;where the cols it required weren't in scope
(t/is
  (=plan-file
    "push-semi-and-anti-joins-down"
    (plan-sql "SELECT
              x.foo
              FROM
              x,
              y
              WHERE
              EXISTS (
              SELECT z.bar
              FROM z
              WHERE
              z.bar = x.foo
              AND
              z.baz = y.biz
              )"))))

(defn- ldt [s] (LocalDateTime/parse s))

(deftest test-timestamp-literal
  (t/are
    [sql expected]
    (= expected (plan-expr sql))
    "TIMESTAMP '3000-03-15 20:40:31'" (ldt "3000-03-15T20:40:31")
    "TIMESTAMP '3000-03-15 20:40:31.11'" (ldt "3000-03-15T20:40:31.11")
    "TIMESTAMP '3000-03-15 20:40:31.2222'" (ldt "3000-03-15T20:40:31.2222")
    "TIMESTAMP '3000-03-15 20:40:31.44444444'" (ldt "3000-03-15T20:40:31.44444444")
    "TIMESTAMP '3000-03-15 20:40:31+03:44'" #time/zoned-date-time "3000-03-15T20:40:31+03:44"
    "TIMESTAMP '3000-03-15 20:40:31.12345678+13:12'" #time/zoned-date-time "3000-03-15T20:40:31.123456780+13:12"
    "TIMESTAMP '3000-03-15 20:40:31.12345678-14:00'" #time/zoned-date-time"3000-03-15T20:40:31.123456780-14:00"
    "TIMESTAMP '3000-03-15 20:40:31.12345678+14:00'" #time/zoned-date-time"3000-03-15T20:40:31.123456780+14:00"
    "TIMESTAMP '3000-03-15 20:40:31-11:44'" #time/zoned-date-time "3000-03-15T20:40:31-11:44"
    "TIMESTAMP '3000-03-15T20:40:31-11:44'" #time/zoned-date-time "3000-03-15T20:40:31-11:44"
    "TIMESTAMP '3000-03-15T20:40:31Z'" #time/zoned-date-time "3000-03-15T20:40:31Z"
    "TIMESTAMP '3000-04-15T20:40:31+01:00[Europe/London]'" #time/zoned-date-time "3000-04-15T20:40:31+01:00[Europe/London]"))

(deftest test-time-literal
  (t/are
    [sql expected]
    (= expected (plan-expr sql))
    "TIME '20:40:31'" #time/time "20:40:31"
    "TIME '20:40:31.467'" #time/time "20:40:31.467"
    "TIME '20:40:31.932254'" #time/time "20:40:31.932254"
    "TIME '20:40:31-03:44'" #time/offset-time "20:40:31-03:44"
    "TIME '20:40:31+03:44'" #time/offset-time "20:40:31+03:44"
    "TIME '20:40:31.467+14:00'" #time/offset-time "20:40:31.467+14:00"))

(deftest date-literal
  (t/are
    [sql expected]
    (= expected (plan-expr sql))
    "DATE '3000-03-15'" #time/date "3000-03-15"))

(deftest test-system-time-queries

  (t/testing "AS OF"
    (t/is
      (=plan-file
        "system-time-as-of"
        (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"))))

  (t/testing "FROM A to B"

    (t/is
      (=plan-file
        "system-time-from-a-to-b"
        (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"))))


  (t/testing "BETWEEN A AND B"
    (t/is
      (=plan-file
        "system-time-between-subquery"
        (plan-sql "SELECT (SELECT 4 FROM t1 FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00') FROM t2")))

    (t/is
      (=plan-file
        "system-time-between-lateraly-derived-table"
        (plan-sql "SELECT x.y, y.z FROM x FOR SYSTEM_TIME AS OF DATE '3001-01-01',
                  LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y")))))

(deftest test-valid-time-period-spec-queries

  (t/testing "AS OF"
    (t/is
     (=plan-file
      "valid-time-period-spec-as-of"
      (plan-sql "SELECT foo.bar FROM foo FOR VALID_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "valid-time-period-spec-from-to"
      (plan-sql "SELECT foo.bar FROM foo FOR VALID_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-time-period-spec-between"
      (plan-sql "SELECT 4 FROM t1 FOR VALID_TIME BETWEEN TIMESTAMP '3000-01-01 00:00:00+00:00' AND DATE '3001-01-01'")))))

(deftest test-valid-and-system-time-period-spec-queries
  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-and-system-time-period-spec-between"
      (plan-sql "SELECT 4 FROM t1
                  FOR SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
                  FOR VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'")))))

;; x1 = app-time-start x2 = app-time-end

(deftest test-period-predicates
  (t/are [expected sql] (= expected (plan-expr sql))
    '(and (<= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (>= x2 #time/zoned-date-time "2001-01-01T00:00Z"))
    "foo.VALID_TIME CONTAINS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(and (<= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (>= x2 #time/zoned-date-time "2000-01-01T00:00Z"))
    "foo.VALID_TIME CONTAINS TIMESTAMP '2000-01-01 00:00:00+00:00'"

    ;; also testing all period-predicate permutations
    '(and (< x1 #time/zoned-date-time "2001-01-01T00:00Z")
          (> x2 #time/zoned-date-time "2000-01-01T00:00Z"))
    "foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(and (< x1 x2) (> x2 x1))
    "foo.VALID_TIME OVERLAPS foo.VALID_TIME"

    '(and (< #time/zoned-date-time "2000-01-01T00:00Z" #time/zoned-date-time "2003-01-01T00:00Z")
          (> #time/zoned-date-time "2001-01-01T00:00Z" #time/zoned-date-time "2002-01-01T00:00Z"))
    "PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')
    OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00+00:00', TIMESTAMP '2003-01-01 00:00:00+00:00')"

    '(and (= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (= x2 #time/zoned-date-time "2001-01-01T00:00Z"))
    "foo.SYSTEM_TIME EQUALS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(<= x2 #time/zoned-date-time "2000-01-01T00:00Z")
    "foo.VALID_TIME PRECEDES PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(>= x1 #time/zoned-date-time "2001-01-01T00:00Z")
    "foo.SYSTEM_TIME SUCCEEDS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(= x2 #time/zoned-date-time "2000-01-01T00:00Z")
    "foo.VALID_TIME IMMEDIATELY PRECEDES PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(= x1 #time/zoned-date-time "2001-01-01T00:00Z")
    "foo.VALID_TIME IMMEDIATELY SUCCEEDS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"))

(deftest test-min-long-value-275
  (t/is (= Long/MIN_VALUE (plan-expr "-9223372036854775808"))))

(deftest test-multiple-references-to-temporal-cols
  (t/is
   (=plan-file
    "multiple-references-to-temporal-cols"
    (plan-sql "SELECT foo.xt$valid_from, foo.xt$valid_to, foo.xt$system_from, foo.xt$system_to
                FROM foo FOR SYSTEM_TIME FROM DATE '2001-01-01' TO DATE '2002-01-01'
                WHERE foo.xt$valid_from = 4 AND foo.xt$valid_to > 10
                AND foo.xt$system_from = 20 AND foo.xt$system_to <= 23
                AND foo.VALID_TIME OVERLAPS PERIOD (DATE '2000-01-01', DATE '2004-01-01')"))))

(deftest test-sql-insert-plan
  (t/is (= '[:insert
             {:table "users"}
             [:rename
              {x1 xt$id, x2 name, x5 xt$valid_from}
              [:project
               [x1 x2 {x5 (cast-tstz x3)}]
               [:table [x1 x2 x3] [{x1 ?_0, x2 ?_1, x3 ?_2}]]]]]
           (plan-sql "INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)")
           (plan-sql
            "INSERT INTO users
             SELECT bar.xt$id, bar.name, bar.xt$valid_from
             FROM (VALUES (?, ?, ?)) AS bar(xt$id, name, xt$valid_from)")
           (plan-sql
            "INSERT INTO users (xt$id, name, xt$valid_from)
             SELECT bar.xt$id, bar.name, bar.xt$valid_from
             FROM (VALUES (?, ?, ?)) AS bar(xt$id, name, xt$valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-309"
                    (plan-sql "INSERT INTO customer (xt$id, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        "#309")

  (t/is (=plan-file "test-sql-insert-plan-398"
                    (plan-sql "INSERT INTO foo (xt$id, xt$valid_from) VALUES ('foo', DATE '2018-01-01')"))))

(deftest test-sql-delete-plan
  (t/is (=plan-file "test-sql-delete-plan"
                    (plan-sql "DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' TO DATE '9999-12-31' AS u WHERE u.id = ?"))))

(deftest test-sql-update-plan
  (t/is (=plan-file "test-sql-update-plan"
                    (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' TO DATE '9999-12-31' AS u SET first_name = 'Sue' WHERE u.id = ?")))

  (t/is (=plan-file "test-sql-update-plan-with-column-references"
                    (plan-sql "UPDATE foo SET bar = foo.baz"
                              {:default-all-valid-time? true})))

  (t/is (=plan-file "test-sql-update-plan-with-period-references"
                    (plan-sql "UPDATE foo SET bar = (foo.SYSTEM_TIME OVERLAPS foo.VALID_TIME)"
                              {:default-all-valid-time? true}))))

(deftest dml-target-table-alises
  (t/is (= (plan-sql "UPDATE t1 AS u SET col1 = 30")
           (plan-sql "UPDATE t1 AS SET SET col1 = 30")
           (plan-sql "UPDATE t1 u SET col1 = 30")
           (plan-sql "UPDATE t1 SET col1 = 30"))
        "UPDATE")

  (t/is (= (plan-sql "DELETE FROM t1 AS u WHERE u.col1 = 30")
           (plan-sql "DELETE FROM t1 u WHERE u.col1 = 30")
           (plan-sql "DELETE FROM t1 WHERE t1.col1 = 30"))
        "DELETE"))

(deftest test-remove-names-359
  (t/is
   (=plan-file
    "test-remove-names-359-single-ref"
    (plan-sql "SELECT (SELECT x.bar FROM z) FROM x")))

  (t/is
   (=plan-file
    "test-remove-names-359-multiple-ref"
    (plan-sql "SELECT (SELECT x.bar FROM (SELECT x.bar FROM z) AS y) FROM x"))))

(deftest test-system-time-period-predicate
  (t/is
    (=plan-file
      "test-system-time-period-predicate-full-plan"
      (plan-sql
        "SELECT foo.name, bar.name
        FROM foo, bar
        WHERE foo.SYSTEM_TIME OVERLAPS bar.SYSTEM_TIME"))))

(deftest test-valid-time-correlated-subquery
  (t/is
   (=plan-file
    "test-valid-time-correlated-subquery-where"
    (plan-sql
     "SELECT (SELECT foo.name
        FROM foo
        WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM bar")))

  (t/is
   (=plan-file
    "test-valid-time-correlated-subquery-projection"
    (plan-sql
     "SELECT (SELECT (foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM foo)
        FROM bar"))))

(deftest test-derived-columns-with-periods
  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-predicate"
    (plan-sql
     "SELECT f.VALID_TIME OVERLAPS f.SYSTEM_TIME
        FROM foo
        AS f (xt$system_from, xt$system_to, xt$valid_from, xt$valid_to)")))

  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-specs"
    (plan-sql
     "SELECT f.bar
        FROM foo
        FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
        FOR VALID_TIME AS OF CURRENT_TIMESTAMP
        AS f (bar)"))))

(deftest test-for-all-valid-time-387
  (t/is
   (=plan-file
    "test-for-all-valid-time-387-query"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR ALL VALID_TIME")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-update"
    (plan-sql
     "UPDATE users FOR ALL VALID_TIME SET first_name = 'Sue'")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-delete"
    (plan-sql
     "DELETE FROM users FOR ALL VALID_TIME"))))

(deftest test-for-all-system-time-404
  (t/is
   (=plan-file
    "test-for-all-system-time-404"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR ALL SYSTEM_TIME"))))

(deftest test-period-specs-with-subqueries-407

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-system-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR ALL SYSTEM_TIME) as tmp")))

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-app-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR VALID_TIME AS OF CURRENT_TIMESTAMP) as tmp")))

  (t/is
   (=plan-file
    "test-period-specs-with-dml-subqueries-and-defaults-407" ;;also #424
    (plan-sql "INSERT INTO prop_owner (xt$id, customer_number, property_number, xt$valid_from, xt$valid_to)
                SELECT 1,
                145,
                7797, DATE '1998-01-03', tmp.app_start
                FROM
                (SELECT MIN(Prop_Owner.xt$system_from) AS app_start
                FROM Prop_Owner
                FOR ALL SYSTEM_TIME
                WHERE Prop_Owner.id = 1) AS tmp"
              {:default-all-valid-time? false}))))

(deftest parenthesized-joined-tables-are-unboxed-502
  (t/is (= (plan-sql "SELECT 1 FROM ( tab0 JOIN tab2 ON TRUE )")
           (plan-sql "SELECT 1 FROM tab0 JOIN tab2 ON TRUE"))))

(deftest test-with-clause
  (t/is (=plan-file
          "test-with-clause"
          (plan-sql "WITH foo AS (SELECT bar.id FROM bar WHERE bar.id = 5)
                    SELECT foo.id, baz.id
                    FROM foo, foo AS baz"))))


(deftest test-delimited-identifiers-in-insert-column-list-2549
  (t/is (=plan-file
          "test-delimited-identifiers-in-insert-column-list-2549"
          (plan-sql
            "INSERT INTO posts (\"xt$id\", \"user_id\") VALUES (1234, 5678)"))))

(deftest test-table-period-specification-ordering-2260
  (let [v-s (plan-sql
              "SELECT foo.bar
              FROM foo
              FOR ALL VALID_TIME
              FOR ALL SYSTEM_TIME")
        s-v (plan-sql
              "SELECT foo.bar
              FROM foo
              FOR ALL SYSTEM_TIME
              FOR ALL VALID_TIME")]

    (t/is (=plan-file "test-table-period-specification-ordering-2260-v-s" v-s))

    (t/is (=plan-file "test-table-period-specification-ordering-2260-s-v" s-v))

    (t/is (= v-s s-v))))
