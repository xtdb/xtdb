(ns core2.sql.plan-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.java.io :as io]
            [core2.edn :as edn] ;; Enables data literals
            [core2.operator :as op]
            [core2.sql.parser :as p]
            [core2.sql.plan :as plan]))

(defn plan-sql
  ([sql] (plan-sql sql {:decorrelate? true}))
  ([sql opts]
   (let [tree (p/parse sql :directly_executable_statement)
         {errs :errs :as plan} (plan/plan-query tree opts)]
     (when (seq errs)
       (println sql))
     (assert (empty? errs) errs)
     #_(assoc (select-keys plan [:fired-rules :plan]) :tree tree) ;; Debug Tool
     (:plan plan))))

(def regen-expected-files? false)

(defmethod t/assert-expr '=plan-file [msg form]
  `(let [exp-plan-file-name# ~(nth form 1)
         exp-plan-file-path# (format "core2/sql/plan_test_expectations/%s.edn" exp-plan-file-name#)
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
             (str (io/resource "core2/sql/plan_test_expectations/") exp-plan-file-name# ".edn")
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
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")))

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
                   "subquery-in-join-uncorellated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo)"))))

      (->> "correlated subquery"
           (t/is (=plan-file
                   "subquery-in-join-corellated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"))))

      (->> "correlated equalty subquery" ;;TODO unable to decorr, need to be able to pull the select over the max-1-row
           (t/is (=plan-file
                   "subquery-in-join-corellated-equality-subquery"
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)")))))))

(t/deftest parameters-in-e-resolved-from-r-test
  (t/are [expected apply-columns]
         (= expected (plan/parameters-in-e-resolved-from-r? apply-columns))
         false {}
         true {'x1 '?x2}
         false {'?x3 '?x4}
         true {'?x3 '?x3
                'x4 '?x5}))


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
          (plan-sql "SELECT u.a[0] AS first_el FROM u")))

  (t/is (=plan-file
          "test-array-element-reference-107-2"
          (plan-sql "SELECT u.b[u.a[0]] AS dyn_idx FROM u"))))

(t/deftest test-current-time-111
  (t/is (=plan-file
          "test-current-time-111"
          (plan-sql "
                    SELECT u.a,
                    CURRENT_TIME, CURRENT_TIME(2),
                    CURRENT_DATE,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(4),
                    LOCALTIME, LOCALTIME(6),
                    LOCALTIMESTAMP, LOCALTIMESTAMP(9)
                    FROM u"))))

(t/deftest test-dynamic-parameters-103
  (t/is (=plan-file
          "test-dynamic-parameters-103-1"
          (plan-sql "SELECT foo.a FROM foo WHERE foo.b = ? AND foo.c = ?")))

  (t/is (=plan-file
          "test-dynamic-parameters-103-2"
          (plan-sql "SELECT foo.a
                    FROM foo, (SELECT bar.b FROM bar WHERE bar.c = ?) bar (b)
                    WHERE foo.b = ? AND foo.c = ?"))))

(t/deftest test-order-by-null-handling-159
  (t/is (=plan-file
          "test-order-by-null-handling-159-1"
           (plan-sql "SELECT foo.a FROM foo ORDER BY foo.a NULLS FIRST")))

  (t/is (=plan-file
          "test-order-by-null-handling-159-2"
          (plan-sql "SELECT foo.a FROM foo ORDER BY foo.a NULLS LAST"))))

(defn- plan-expr [sql]
  (let [plan (plan-sql (format "SELECT %s t FROM foo WHERE foo.a = 42" sql))
        [expr] (filter (fn [form] (and (list? form) (symbol? (first form)))) (tree-seq seqable? seq plan))]
    expr))

(t/deftest test-trim-expr
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "TRIM(foo.a)" '(trim x1 "BOTH" " ")

    "TRIM(LEADING FROM foo.a)" '(trim x1 "LEADING" " ")
    "TRIM(LEADING '$' FROM foo.a)" '(trim x1 "LEADING" "$")
    "TRIM(LEADING foo.b FROM foo.a)" '(trim x2 "LEADING" x1)

    "TRIM(TRAILING FROM foo.a)" '(trim x1 "TRAILING" " ")
    "TRIM(TRAILING '$' FROM foo.a)" '(trim x1 "TRAILING" "$")
    "TRIM(TRAILING foo.b FROM foo.a)" '(trim x2 "TRAILING" x1)

    "TRIM(BOTH FROM foo.a)" '(trim x1 "BOTH" " ")
    "TRIM(BOTH '$' FROM foo.a)" '(trim x1 "BOTH" "$")
    "TRIM(BOTH foo.b FROM foo.a)" '(trim x2 "BOTH" x1)

    "TRIM(BOTH '😎' FROM foo.a)" '(trim x1 "BOTH" "😎")))

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
  (t/is (= '(character-length x1 "CHARACTERS") (plan-expr "CHARACTER_LENGTH(foo.a)")))
  (t/is (= '(character-length x1 "CHARACTERS") (plan-expr "CHARACTER_LENGTH(foo.a USING CHARACTERS)")))
  (t/is (= '(character-length x1 "OCTETS") (plan-expr "CHARACTER_LENGTH(foo.a USING OCTETS)"))))

(t/deftest test-char-length-alias
  (t/is (= '(character-length x1 "CHARACTERS") (plan-expr "CHAR_LENGTH(foo.a)")) "CHAR_LENGTH alias works")
  (t/is (= '(character-length x1 "CHARACTERS") (plan-expr "CHAR_LENGTH(foo.a USING CHARACTERS)")) "CHAR_LENGTH alias works")
  (t/is (= '(character-length x1 "OCTETS") (plan-expr "CHAR_LENGTH(foo.a USING OCTETS)")) "CHAR_LENGTH alias works"))

(t/deftest test-octet-length-expr
  (t/is (= '(octet-length x1) (plan-expr "OCTET_LENGTH(foo.a)"))))

(t/deftest test-position-expr
  (t/is (= '(position x1 x2 "CHARACTERS") (plan-expr "POSITION(foo.a IN foo.b)")))
  (t/is (= '(position x1 x2 "CHARACTERS") (plan-expr "POSITION(foo.a IN foo.b USING CHARACTERS)")))
  (t/is (= '(position x1 x2 "OCTETS") (plan-expr "POSITION(foo.a IN foo.b USING OCTETS)"))))

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
    "foo.a DAY TO SECOND" '(multi-field-interval x1 "DAY" 2 "SECOND" 6)))

(deftest test-interval-abs
  (t/are [sql expected]
    (= expected (plan-expr sql))

    "ABS(foo.a)" '(abs x1)
    "ABS(1 YEAR)" '(abs (single-field-interval 1 "YEAR" 2 0))))
