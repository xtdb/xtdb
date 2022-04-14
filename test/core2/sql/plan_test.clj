(ns core2.sql.plan-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.sql :as sql]
            [core2.sql.plan :as plan]))

(defn plan-sql [sql]
  (let [tree (sql/parse sql)
        {errs :errs :as plan} (plan/plan-query tree)]
    (assert (empty? errs) errs)
    #_(assoc (select-keys plan [:fired-rules :plan]) :tree tree) ;; Debug Tool
    (:plan plan)))

(t/deftest test-basic-queries
  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:join [{x2 x4}]
               [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
               [:rename {name x4, birthdate x5}
                [:scan [name {birthdate (= birthdate 1960)}]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960")))

  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:join [{x2 x4}]
               [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
               [:rename {name x4, birthdate x5}
                [:scan [name {birthdate (and (< birthdate 1960) (> birthdate 1950))}]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950")))

  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:join [{x2 x4}]
               [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
               [:rename {name x4, birthdate x5}
                [:scan [{name (= name "Foo")} {birthdate (< birthdate 1960)}]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'")))

  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:join [{x2 x4}]
               [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
               [:rename {name x4, birthdate x5}
                [:scan [name {birthdate (= birthdate 1960)}]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name")))

  (t/is (= '[:rename {x4 movieTitle}
             [:project [x4]
              [:join [{x1 x4} {x2 x5}]
               [:rename {title x1, movieYear x2} [:scan [title movieYear]]]
               [:rename {movieTitle x4, year x5} [:scan [movieTitle year]]]]]]
           (plan-sql "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")))

  (t/is (= '[:rename {x4 movieTitle}
             [:project [x4]
              [:left-outer-join [{x1 x4} {x2 x5}]
               [:rename {title x1, movieYear x2} [:scan [title movieYear]]]
               [:rename {movieTitle x4, year x5} [:scan [movieTitle year]]]]]]
           (plan-sql "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")))

  (t/is (= '[:rename {x3 title}
             [:project [x3]
              [:join [{x1 x3}]
               [:rename {title x1} [:scan [title]]]
               [:rename {title x3} [:scan [title]]]]]]
           (plan-sql "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)")))

  (t/is (= '[:rename {x1 title}
             [:project [x1]
              [:left-outer-join [{x1 x3}]
               [:rename {title x1} [:scan [title]]]
               [:rename {title x3} [:scan [title]]]]]]
           (plan-sql "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)")))

  (t/is (= '[:rename {x1 name, x8 $column_2$}
             [:project [x1 x8]
              [:select (< x9 1930)
               [:group-by [x1 {x8 (sum x4)} {x9 (min x6)}]
                [:join [{x2 x5}]
                 [:rename {name x1, cert x2} [:scan [name cert]]]
                 [:rename {length x4, producer x5, year x6}
                  [:scan [length producer year]]]]]]]]
           (plan-sql "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930")))

  (t/is (= '[:rename {x3 $column_1$}
             [:group-by [{x3 (sum x1)}] [:rename {length x1} [:scan [length]]]]]
           (plan-sql "SELECT SUM(m.length) FROM Movie AS m")))

  (t/is (= '[:scan [name]]
           (plan-sql "SELECT * FROM StarsIn AS si(name)")))

  (t/is (= '[:rename {x1 bar}
             [:rename {name x1}
              [:scan [name]]]]
           (plan-sql "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)")))

  (t/is (= (plan-sql "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname")
           '[:select (= name lastname) [:scan [name lastname]]]))

  (t/is (= '[:rename {x1 movieTitle}
             [:distinct
              [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT DISTINCT si.movieTitle FROM StarsIn AS si")))

  (t/is (= '[:rename {x1 name}
             [:difference
              [:rename {name x1} [:scan [name]]]
              [:rename {name x1} [:scan [name]]]]]
           (plan-sql "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si")))

  (t/is (= '[:rename {x1 name}
             [:union-all
              [:rename {name x1} [:scan [name]]]
              [:rename {name x1} [:scan [name]]]]]
           (plan-sql "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si")))

  (t/is (= '[:rename {x1 name}
             [:intersect
              [:rename {name x1} [:scan [name]]]
              [:rename {name x1} [:scan [name]]]]]
           (plan-sql "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si")))

  (t/is (= '[:rename
             {x1 movieTitle}
             [:distinct
              [:union-all
               [:rename {movieTitle x1} [:scan [movieTitle]]]
               [:rename {name x1} [:scan [name]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si")))

  (t/is (= '[:rename {x1 name}
             [:order-by [{x1 :asc}]
              [:distinct
               [:union-all
                [:rename {name x1} [:scan [name]]]
                [:rename {name x1} [:scan [name]]]]]]]
           (plan-sql "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name")))

  (t/is (= '[:rename {x1 movieTitle}
             [:top {:limit 10}
              [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY")))

  (t/is (= '[:rename {x1 movieTitle}
             [:top {:skip 5} [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS")))

  (t/is (= '[:rename {x1 movieTitle}
             [:top {:skip 5, :limit 10}
              [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")))

  (t/is (= '[:rename {x1 movieTitle}
             [:order-by [{x1 :asc}] [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle")))

  (t/is (= '[:rename {x1 movieTitle}
             [:top {:skip 100}
              [:order-by [{x1 :asc}]
               [:rename {movieTitle x1} [:scan [movieTitle]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS")))

  (t/is (= '[:rename {x1 movieTitle}
             [:order-by [{x1 :desc}]
              [:rename {movieTitle x1} [:scan [movieTitle]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC")))

  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:order-by [{x4 :desc} {x1 :asc}]
               [:map [{x4 (= x2 "foo")}]
                [:rename {movieTitle x1, year x2} [:scan [movieTitle year]]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle")))

  (t/is (= '[:rename {x1 movieTitle}
             [:project [x1]
              [:order-by [{x2 :asc}]
               [:rename {movieTitle x1, year x2} [:scan [movieTitle year]]]]]]
           (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year")))

  (t/is (= '[:rename {x3 $column_1$}
             [:order-by
              [{x3 :asc}]
              [:project [{x3 (= x1 "foo")}] [:rename {year x1} [:scan [year]]]]]]
           (plan-sql "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'")))

  (t/is (= '[:rename {x3 name}
             [:project [x3]
              [:unwind {x3 x1} {} [:rename {films x1} [:scan [films]]]]]]
           (plan-sql "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)")))

  (t/is (= '[:rename {x1 films, x3 $column_2$}
             [:project [x1 x3]
              [:unwind {x3 x1} {} [:rename {films x1} [:scan [films]]]]]]
           (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film")))

  (t/is (= '[:rename {x1 films, x3 $column_2$, x4 $column_3$}
             [:project [x1 x3 x4]
              [:unwind {x3 x1} {:ordinality-column x4}
               [:rename {films x1} [:scan [films]]]]]]
           (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film")))

  (t/is (= '[:rename {x1 $column_1$, x2 $column_2$}
             [:table [{x1 1, x2 2} {x1 3, x2 4}]]]
           (plan-sql "VALUES (1, 2), (3, 4)")))

  (t/is (= '[:rename {x1 $column_1$}
             [:table [{x1 1} {x1 2}]]]
           (plan-sql "VALUES 1, 2")))

  (t/is (= '[:rename
             {x7 $column_1$, x8 $column_2$, x9 $column_3$}
             [:project [{x7 (case (+ x1 1) x2 111 x3 222 x4 333 x5 444 555)}
                        {x8 (cond (< x1 (- x2 3)) 111 (<= x1 x2) 222 (< x1 (+ x2 3)) 333 444)}
                        {x9 (case (+ x1 1) x2 222 x3 222 x4 444 (+ x5 1) 444 555)}]
              [:rename {a x1, b x2, c x3, d x4, e x5} [:scan [a b c d e]]]]]
           (plan-sql "SELECT CASE t1.a + 1 WHEN t1.b THEN 111 WHEN t1.c THEN 222 WHEN t1.d THEN 333 WHEN t1.e THEN 444 ELSE 555 END,
                             CASE WHEN t1.a < t1.b - 3 THEN 111 WHEN t1.a <= t1.b THEN 222 WHEN t1.a < t1.b+3 THEN 333 ELSE 444 END,
                             CASE t1.a + 1 WHEN t1.b, t1.c THEN 222 WHEN t1.d, t1.e + 1 THEN 444 ELSE 555 END FROM t1")))

  (t/is (= '[:scan [{a (nil? a)}]]
           (plan-sql "SELECT * FROM t1 WHERE t1.a IS NULL")))
  (t/is (= '[:scan [{a (not (nil? a))}]]
           (plan-sql "SELECT * FROM t1 WHERE t1.a IS NOT NULL")))

  (t/is (= '[:rename {x5 $column_1$}
             [:project [{x5 (coalesce x1 x2 x3)}]
              [:rename {a x1, b x2, c x3} [:scan [a b c]]]]]
        (plan-sql "SELECT COALESCE(t1.a, t1.b, t1.c) FROM t1"))))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(t/deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (= '[:rename {x8 some_column}
               [:project [{x8 (= 1 x5)}]
                [:cross-join
                 [:rename {y x1}
                  [:scan [{y (= y 1)}]]]
                 [:max-1-row
                  [:group-by [{x5 (max x3)}]
                   [:rename {bar x3} [:scan [bar]]]]]]]]
             (plan-sql "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"))))

  (t/testing "Scalar subquery in WHERE"
    (t/is (= '[:rename {x1 some_column}
               [:project [x1]
                [:join [{x1 x5}]
                 [:rename {y x1} [:scan [y]]]
                 [:max-1-row
                  [:group-by [{x5 (max x3)}]
                   [:rename {bar x3} [:scan [bar]]]]]]]]
             (plan-sql "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"))))

  (t/testing "Correlated scalar subquery in SELECT"
    (t/is (= '[:rename {x8 some_column}
               [:project [{x8 (= 1 x5)}]
                [:apply :cross-join {x1 ?x6} #{x5}
                 [:rename {y x1} [:scan [{y (= y 1)}]]]
                 [:max-1-row
                  [:project [{x5 (= x3 ?x6)}] [:rename {bar x3} [:scan [bar]]]]]]]]
             (plan-sql "SELECT (1 = (SELECT foo.bar = x.y FROM foo)) AS some_column FROM x WHERE x.y = 1"))))

  (t/testing "EXISTS in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:semi-join [{x1 x4}]
                 [:rename {y x1, z x2} [:scan [y {z (= z 10.0)}]]]
                 [:rename {z x4} [:scan [z]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10.0"))))

  (t/testing "EXISTS as expression in SELECT"
    (t/is (= '[:rename {x6 $column_1$}
               [:project [x6]
                [:apply :cross-join {x1 ?x9} #{x6}
                 [:rename {y x1, z x2}
                  [:scan [y {z (= z 10)}]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{x6 true}]
                    [:rename {z x4} [:scan [{z (= z ?x9)}]]]]
                   [:table [{x6 false}]]]]]]]
             (plan-sql "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"))))

  (t/testing "NOT EXISTS in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:anti-join [{x1 x4}]
                 [:rename {y x1, z x2} [:scan [y {z (= z 10)}]]]
                 [:rename {z x4} [:scan [z]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"))))

  (t/testing "IN in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:semi-join [{x2 x4}]
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:rename {z x4} [:scan [z]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)")))

    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:semi-join [{x2 x4}]
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:table [{x4 1} {x4 2}]]]]]
             (plan-sql "SELECT x.y FROM x WHERE x.z IN (1, 2)"))))

  (t/testing "NOT IN in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:anti-join [{x2 x4}]
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:rename {z x4} [:scan [z]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"))))

  (t/testing "ALL in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:anti-join [(or (<= x2 x4) (nil? x2) (nil? x4))]
                 [:rename {y x1, z x2}
                  [:scan [y z]]]
                 [:rename {z x4} [:scan [z]]]]]]
            (plan-sql "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"))))

  (t/testing "ANY in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:semi-join
                 [(> (= x2 1) x4)]
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:rename {z x4}
                  [:scan [z]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"))))

  (t/testing "ALL as expression in SELECT"
    (t/is (= '[:rename {x10 $column_1$}
               [:project [{x10 (not x5)}]
                [:apply :cross-join {x1 ?x8} #{x5}
                 [:rename {z x1} [:scan [z]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{x5 true}]
                    [:rename {z x3}
                     [:scan [{z (or (> ?x8 z) (nil? ?x8) (nil? z))}]]]]
                   [:table [{x5 false}]]]]]]]
             (plan-sql "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"))))

  (t/testing "LATERAL derived table"
    (t/is (= '[:rename {x1 y, x3 z}
               [:join [{x1 x3}]
                [:rename {y x1} [:scan [y]]]
                [:rename {z x3} [:scan [z]]]]]
             (plan-sql "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y")))

    (t/is (= '[:scan [{z (= z 1)}]]
             (plan-sql "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"))))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (t/is (= '[:rename {x1 custkey}
               [:project [x1]
                [:select (< 1000000 x6)
                 [:group-by [x1 $row_number$ {x6 (sum x3)}]
                  [:left-outer-join [{x1 x4}]
                   [:map [{$row_number$ (row-number)}]
                    [:rename {custkey x1} [:scan [custkey]]]]
                   [:rename {totalprice x3, custkey x4}
                    [:scan [totalprice custkey]]]]]]]]
             (plan-sql "SELECT c.custkey FROM customer c
                       WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)")))

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (t/is (= '[:rename {x1 country, x2 custno}
               [:semi-join [{x2 x4}]
                [:rename {country x1, custno x2}
                 [:scan [{country (= country "Mexico")} custno]]]
                [:rename {custno x4} [:scan [custno]]]]]
             (plan-sql "SELECT * FROM customers
                       WHERE customers.country = 'Mexico' AND
                       EXISTS (SELECT * FROM orders WHERE customers.custno = orders.custno)")))

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (t/is (= '[:rename {x1 name, x12 $column_2$}
               [:project [x1 x12]
                [:group-by [x1 x2 x3 $row_number$ {x12 (count x11)}]
                 [:map [{x11 1}]
                  [:left-outer-join [{x2 x9}]
                   [:map [{$row_number$ (row-number)}]
                    [:anti-join [{x3 x5}]
                     [:rename {name x1, custno x2, country x3}
                      [:scan [name custno country]]]
                     [:rename {country x5} [:scan [country]]]]]
                   [:rename {custno x9} [:scan [custno]]]]]]]]
             (plan-sql "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
                       FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)")))

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (t/is (= '[:rename {x1 name, x4 course}
               [:project [x1 x4]
                [:select (= x6 x11)
                 [:group-by [x1 x2 x4 x5 x6 $row_number$ {x11 (min x8)}]
                  [:left-outer-join [{x2 x9}]
                   [:map [{$row_number$ (row-number)}]
                    [:join [{x2 x5}]
                     [:rename {name x1, id x2} [:scan [name id]]]
                     [:rename {course x4, sid x5, grade x6}
                      [:scan [course sid grade]]]]]
                   [:rename {grade x8, sid x9} [:scan [grade sid]]]]]]]]
             (plan-sql "SELECT s.name, e.course
                       FROM students s, exams e
                       WHERE s.id = e.sid AND
                       e.grade = (SELECT MIN(e2.grade)
                       FROM exams e2
                       WHERE s.id = e2.sid)")))

    (t/is (= '[:rename {x1 name, x6 course}
               [:project [x1 x6]
                [:select (>= x8 x17)
                 [:map [{x17 (+ x15 1)}]
                  [:group-by [x1 x2 x3 x4 x6 x7 x8 $row_number$ {x15 (avg x10)}]
                   [:left-outer-join
                    [(or (= x2 x11) (and (= x12 x3) (> x4 x13)))]
                    [:map [{$row_number$ (row-number)}]
                     [:join [{x2 x7}]
                      [:rename {name x1, id x2, major x3, year x4}
                       [:scan [name id {major (or (= major "CS") (= major "Games Eng"))} year]]]
                      [:rename {course x6, sid x7, grade x8}
                       [:scan [course sid grade]]]]]
                    [:rename {grade x10, sid x11, curriculum x12, date x13}
                     [:scan [grade sid curriculum date]]]]]]]]]
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
           (t/is (= '[:rename {x1 a}
                      [:project [x1]
                       [:join [{x3 x5}]
                        [:join []
                         [:rename {a x1} [:scan [a]]]
                         [:rename {c x3} [:scan [c]]]]
                        [:max-1-row
                         [:rename {b x5} [:scan [b]]]]]]]
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo)"))))

      (->> "correlated subquery"
           (t/is (= '[:rename {x1 a}
                      [:project [x1]
                       [:semi-join [{x3 x6} {x4 x7}]
                        [:join []
                         [:rename {a x1}
                          [:scan [a]]]
                         [:rename {c x3, b x4} [:scan [c b]]]]
                        [:rename {b x6, a x7} [:scan [b a]]]]]]
                    (plan-sql "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"))))

      (->> "correlated equalty subquery" ;;TODO unable to decorr, need to be able to pull the select over the max-1-row
           (t/is (= '[:rename {x1 a}
                      [:project [x1]
                       [:select (= x3 x6)
                        [:apply :cross-join
                         {x4 ?x9}
                         #{x6}
                         [:join []
                          [:rename {a x1} [:scan [a]]]
                          [:rename {c x3, b x4} [:scan [c b]]]]
                         [:max-1-row
                          [:rename {b x6, a x7}
                           [:scan [b {a (= a ?x9)}]]]]]]]]
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
  (t/is
    (=
     '[:rename {x1 a}
       [:project [x1]
        [:select (or x6 (= x2 42))
         [:apply
          :cross-join
          {x1 ?x9}
          #{x6}
          [:rename {a x1, b x2} [:scan [a b]]]
          [:top {:limit 1}
           [:union-all
            [:project [{x6 true}]
             [:select (= ?x9 x4) [:table [{x4 1} {x4 2}]]]]
            [:table [{x6 false}]]]]]]]]
       (plan-sql "select f.a from foo f where f.a in (1,2) or f.b = 42"))
    "should not be decorrelated")
  (t/is
    (=
     '[:rename {x1 a}
       [:project [x1]
        [:cross-join
         [:rename {a x1} [:scan [a]]]
         [:select (= true x5)
          [:top {:limit 1}
           [:union-all
            [:project [{x5 true}]
             [:rename {c x3} [:scan [c]]]]
            [:table [{x5 false}]]]]]]]]
     (plan-sql "select f.a from foo f where true = (EXISTS (SELECT foo.c from foo))"))
    "should be decorrelated as a cross join, not a semi/anti join"))

(deftest multiple-ins-in-where-clause
  (t/is
    (=
     '[:rename {x1 a}
       [:project [x1]
        [:semi-join [{x2 x8}]
         [:semi-join [{x1 x4}]
          [:rename {a x1, b x2}
           [:scan [{a (= a 42)} b]]]
          [:table [{x4 1} {x4 2}]]]
         [:table [{x8 3} {x8 4}]]]]]
     (plan-sql "select f.a from foo f where f.a in (1,2) AND f.a = 42 AND f.b in (3,4)"))))

(deftest deeply-nested-correlated-query ;;TODO broken
  (t/is
    (=
     '[:rename
       {x1 A, x2 B}
       [:project
        [x1 x2]
        [:apply :semi-join {x2 ?x15, x4 ?x16} #{}
         [:cross-join
          [:rename {A x1, B x2} [:scan [A B]]]
          [:rename {C x4} [:scan [C]]]]
         [:semi-join [(= x10 ?x13) {x7 x9}]
          [:rename {A x6, B x7}
           [:scan [{A (= A ?x15)} B]]]
          [:rename {A x9, B x10} [:scan [A B]]]]]]]
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
  (t/is (= '[:rename {x3 first_el}
             [:project [{x3 (nth x1 0)}]
              [:rename {a x1}
               [:scan [a]]]]]
           (plan-sql "SELECT u.a[0] AS first_el FROM u")))

  (t/is (= '[:rename {x4 dyn_idx}
             [:project [{x4 (nth x1 (nth x2 0))}]
              [:rename {b x1, a x2}
               [:scan [b a]]]]]
           (plan-sql "SELECT u.b[u.a[0]] AS dyn_idx FROM u"))))
