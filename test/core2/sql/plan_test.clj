(ns core2.sql.plan-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]
            [core2.sql.plan :as plan]))

(defmacro valid? [sql expected]
  `(let [tree# (sql/parse ~sql)
         {errs# :errs plan# :plan} (plan/plan-query tree#)]
     (t/is (= [] (vec errs#)))
     (t/is (= ~expected plan#))
     {:tree tree# :plan plan#}))

(t/deftest test-basic-queries
  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:join {x2 x4}
              [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
              [:rename {name x4, birthdate x5}
               [:select (= birthdate 1960)
                [:scan [name {birthdate (= birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:join {x2 x4}
              [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
              [:rename {name x4, birthdate x5}
               [:select (and (< birthdate 1960) (> birthdate 1950))
                [:scan [name {birthdate (and (< birthdate 1960) (> birthdate 1950))}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:join {x2 x4}
              [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
              [:rename {name x4, birthdate x5}
               [:select (and (< birthdate 1960) (= name "Foo"))
                [:scan [{name (= name "Foo")} {birthdate (< birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:join {x2 x4}
              [:rename {movieTitle x1, starName x2} [:scan [movieTitle starName]]]
              [:rename {name x4, birthdate x5}
               [:select (= birthdate 1960)
                [:scan [name {birthdate (= birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          '[:rename {x4 movieTitle}
            [:project [x4]
             [:select (= x5 x2)
              [:join {x1 x4}
               [:rename {title x1, movieYear x2} [:scan [title movieYear]]]
               [:rename {movieTitle x4, year x5} [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          '[:rename {x4 movieTitle}
            [:project [x4]
             [:select (= x5 x2)
              [:left-outer-join {x1 x4}
               [:rename {title x1, movieYear x2} [:scan [title movieYear]]]
               [:rename {movieTitle x4, year x5} [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)"
          '[:rename {x3 title}
            [:project [x3]
             [:join {x1 x3}
              [:rename {title x1} [:scan [title]]]
              [:rename {title x3} [:scan [title]]]]]])

  (valid? "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)"
          '[:rename {x1 title}
            [:project [x1]
             [:left-outer-join {x1 x3}
              [:rename {title x1} [:scan [title]]]
              [:rename {title x3} [:scan [title]]]]]])

  (valid? "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930"
          '[:rename {x1 name, x8 $column_2$}
            [:project [x1 x8]
             [:select (< x9 1930)
              [:group-by [x1 {x8 (sum x4)} {x9 (min x6)}]
               [:join {x2 x5}
                [:rename {name x1, cert x2} [:scan [name cert]]]
                [:rename {length x4, producer x5, year x6}
                 [:scan [length producer year]]]]]]]])

  (valid? "SELECT SUM(m.length) FROM Movie AS m"
          '[:rename {x3 $column_1$}
            [:group-by [{x3 (sum x1)}] [:rename {length x1} [:scan [length]]]]])

  (valid? "SELECT * FROM StarsIn AS si(name)"
          '[:scan [name]])

  (valid? "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
          '[:rename {x1 bar}
            [:rename {name x1}
             [:scan [name]]]])

  (valid? "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname"
          '[:select (= name lastname) [:scan [name lastname]]])

  (valid? "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
          '[:rename {x1 movieTitle}
            [:distinct
             [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
          '[:rename {x1 name}
            [:difference
             [:rename {name x1} [:scan [name]]]
             [:rename {name x1} [:scan [name]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
          '[:rename {x1 name}
            [:union-all
             [:rename {name x1} [:scan [name]]]
             [:rename {name x1} [:scan [name]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
          '[:rename {x1 name}
            [:intersect
             [:rename {name x1} [:scan [name]]]
             [:rename {name x1} [:scan [name]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
          '[:rename
            {x1 movieTitle}
            [:distinct
             [:union-all
              [:rename {movieTitle x1} [:scan [movieTitle]]]
              [:rename {name x1} [:scan [name]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
          '[:rename {x1 name}
            [:order-by [{x1 :asc}]
             [:distinct
              [:union-all
               [:rename {name x1} [:scan [name]]]
               [:rename {name x1} [:scan [name]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
          '[:rename {x1 movieTitle}
            [:top {:limit 10}
             [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
          '[:rename {x1 movieTitle}
            [:top {:skip 5} [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"
          '[:rename {x1 movieTitle}
            [:top {:skip 5, :limit 10}
             [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
          '[:rename {x1 movieTitle}
            [:order-by [{x1 :asc}] [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
          '[:rename {x1 movieTitle}
            [:top {:skip 100}
             [:order-by [{x1 :asc}]
              [:rename {movieTitle x1} [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
          '[:rename {x1 movieTitle}
            [:order-by [{x1 :desc}]
             [:rename {movieTitle x1} [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:order-by [{x4 :desc} {x1 :asc}]
              [:map [{x4 (= x2 "foo")}]
               [:rename {movieTitle x1, year x2} [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"
          '[:rename {x1 movieTitle}
            [:project [x1]
             [:order-by [{x2 :asc}]
              [:rename {movieTitle x1, year x2} [:scan [movieTitle year]]]]]])

  (valid? "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"
          '[:rename {x3 $column_1$}
            [:order-by
             [{x3 :asc}]
             [:project [{x3 (= x1 "foo")}] [:rename {year x1} [:scan [year]]]]]])

  (valid? "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)"
          '[:rename {x3 name}
            [:project [x3]
             [:unwind {x3 x1} {} [:rename {films x1} [:scan [films]]]]]])

  (valid? "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film"
          '[:rename {x1 films, x3 $column_2$}
            [:project [x1 x3]
             [:unwind {x3 x1} {} [:rename {films x1} [:scan [films]]]]]])

  (valid? "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
          '[:rename {x1 films, x3 $column_2$, x4 $column_3$}
            [:project [x1 x3 x4]
             [:unwind {x3 x1} {:ordinality-column x4}
              [:rename {films x1} [:scan [films]]]]]]))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(t/deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (valid? "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:rename {x7 some_column}
              [:project [{x7 (= 1 x5)}]
               [:cross-join
                [:rename {y x1}
                 [:select (= y 1)
                  [:scan [{y (= y 1)}]]]]
                [:max-1-row
                 [:group-by [{x5 (max x3)}]
                  [:rename {bar x3} [:scan [bar]]]]]]]]))

  (t/testing "Scalar subquery in WHERE"
    (valid? "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"
            '[:rename {x1 some_column}
              [:project [x1]
               [:join {x1 x5}
                [:rename {y x1} [:scan [y]]]
                [:max-1-row
                 [:group-by [{x5 (max x3)}]
                  [:rename {bar x3} [:scan [bar]]]]]]]]))

  (t/testing "Correlated scalar subquery in SELECT"
    (valid? "SELECT (1 = (SELECT foo.bar = x.y FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:rename {x8 some_column}
              [:project [{x8 (= 1 x5)}]
               [:apply :cross-join {x1 ?x6} #{x5}
                [:rename {y x1} [:select (= y 1) [:scan [{y (= y 1)}]]]]
                [:max-1-row
                 [:project [{x5 (= x3 ?x6)}] [:rename {bar x3} [:scan [bar]]]]]]]]))

  (t/testing "EXISTS in WHERE"
    (valid? "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x1 y}
              [:project [x1]
               [:semi-join {x1 x4}
                [:rename {y x1, z x2} [:select (= z 10) [:scan [y {z (= z 10)}]]]]
                [:rename {z x4} [:scan [z]]]]]]))

  (t/testing "EXISTS as expression in SELECT"
    (valid? "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
            '[:rename {x6 $column_1$}
              [:project [x6]
               [:apply :cross-join {x1 ?x9} #{x6}
                [:rename {y x1, z x2}
                 [:select (= z 10) [:scan [y {z (= z 10)}]]]]
                [:top {:limit 1}
                 [:union-all
                  [:project [{x6 true}]
                   [:rename {z x4} [:select (= z ?x9) [:scan [{z (= z ?x9)}]]]]]
                  [:table [{x6 false}]]]]]]]))

  (t/testing "NOT EXISTS in WHERE"
    (valid? "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x1 y}
              [:project [x1]
               [:anti-join {x1 x4}
                [:rename {y x1, z x2} [:select (= z 10) [:scan [y {z (= z 10)}]]]]
                [:rename {z x4} [:scan [z]]]]]]))

  (t/testing "IN in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
            '[:rename {x1 y}
              [:project [x1]
               [:semi-join {x2 x4}
                [:rename {y x1, z x2} [:scan [y z]]]
                [:rename {z x4} [:scan [z]]]]]]))

  (t/testing "NOT IN in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
            '[:rename {x1 y}
              [:project [x1]
               [:anti-join {x2 x4}
                [:rename {y x1, z x2} [:scan [y z]]]
                [:rename {z x4} [:scan [z]]]]]]))

  (t/testing "ALL in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
            '[:rename {x1 y}
              [:project [x1]
               [:apply :anti-join {x2 ?x9} #{}
                [:rename {y x1, z x2} [:scan [y z]]]
                [:rename {z x4}
                 [:select (or (<= ?x9 z) (nil? ?x9) (nil? z))
                  [:scan [{z (or (<= ?x9 z) (nil? ?x9) (nil? z))}]]]]]]]))

  (t/testing "ANY in WHERE"
    (valid? "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
            '[:rename {x1 y}
              [:project [x1]
               [:apply :semi-join {x2 ?x9} #{}
                [:rename {y x1, z x2} [:scan [y z]]]
                [:rename {z x4}
                 [:select (> (= ?x9 1) z)
                  [:scan [{z (> (= ?x9 1) z)}]]]]]]]))

  (t/testing "ALL as expression in SELECT"
    (valid? "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"
            '[:rename {x10 $column_1$}
              [:project [{x10 (not x5)}]
               [:apply :cross-join {x1 ?x8} #{x5}
                [:rename {z x1} [:scan [z]]]
                [:top {:limit 1}
                 [:union-all
                  [:project [{x5 true}]
                   [:rename {z x3}
                    [:select (or (> ?x8 z) (nil? ?x8) (nil? z))
                     [:scan [{z (or (> ?x8 z) (nil? ?x8) (nil? z))}]]]]]
                  [:table [{x5 false}]]]]]]]))

  (t/testing "LATERAL derived table"
    (valid? "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
            '[:rename {x1 y, x3 z}
              [:join {x1 x3}
               [:rename {y x1} [:scan [y]]]
               [:rename {z x3} [:scan [z]]]]])

    (valid? "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"
            '[:select (= z 1)
              [:scan [{z (= z 1)}]]]))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (valid? "SELECT c.custkey FROM customer c
             WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)"
            '[:rename {x1 custkey}
              [:project [x1]
               [:select (< 1000000 x6)
                [:group-by [x1 $row_number$ {x6 (sum x3)}]
                 [:left-outer-join {x1 x4}
                  [:map [{$row_number$ (row-number)}]
                   [:rename {custkey x1} [:scan [custkey]]]]
                  [:rename {totalprice x3, custkey x4}
                   [:scan [totalprice custkey]]]]]]]])

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (valid? "SELECT * FROM customers
             WHERE customers.country = 'Mexico' AND
                   EXISTS (SELECT * FROM orders WHERE customers.custno = orders.custno)"
            '[:rename {x1 country, x2 custno}
              [:semi-join {x2 x4}
               [:rename {country x1, custno x2}
                [:select (= country "Mexico")
                 [:scan [{country (= country "Mexico")} custno]]]]
               [:rename {custno x4} [:scan [custno]]]]])

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (valid? "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)"
            '[:rename {x1 name, x15 $column_2$}
              [:project [x1 x15]
               [:group-by [x1 x2 x3 $row_number$ {x15 (count x14)}]
                [:map [{x14 1}]
                 [:left-outer-join {x2 x12}
                  [:map [{$row_number$ (row-number)}]
                   [:anti-join {x3 x5}
                    [:rename {name x1, custno x2, country x3}
                     [:scan [name custno country]]]
                    [:rename {country x5} [:scan [country]]]]]
                  [:rename {custno x12} [:scan [custno]]]]]]]])

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (valid? "SELECT s.name, e.course
             FROM students s, exams e
             WHERE s.id = e.sid AND
             e.grade = (SELECT MIN(e2.grade)
                        FROM exams e2
                        WHERE s.id = e2.sid)"
            '[:rename {x1 name, x4 course}
              [:project [x1 x4]
               [:select (= x6 x11)
                [:group-by [x1 x2 x4 x5 x6 $row_number$ {x11 (min x8)}]
                 [:left-outer-join {x2 x9}
                  [:map [{$row_number$ (row-number)}]
                   [:join {x2 x5}
                    [:rename {name x1, id x2} [:scan [name id]]]
                    [:rename {course x4, sid x5, grade x6}
                     [:scan [course sid grade]]]]]
                  [:rename {grade x8, sid x9} [:scan [grade sid]]]]]]]])

    (valid? "SELECT s.name, e.course
             FROM students s, exams e
             WHERE s.id = e.sid AND
                   (s.major = 'CS' OR s.major = 'Games Eng') AND
                   e.grade >= (SELECT AVG(e2.grade) + 1
                               FROM exams e2
                               WHERE s.id = e2.sid OR
                                     (e2.curriculum = s.major AND
                                      s.year > e2.date))"
            '[:rename {x1 name, x6 course}
              [:project [x1 x6]
               [:select (>= x8 x17)
                [:map [{x17 (+ x15 1)}]
                 [:group-by [x1 x2 x3 x4 x6 x7 x8 $row_number$ {x15 (avg x10)}]
                  [:apply :left-outer-join {x2 ?x18, x3 ?x19, x4 ?x20} #{x17}
                   [:map [{$row_number$ (row-number)}]
                    [:join {x2 x7}
                     [:rename {name x1, id x2, major x3, year x4}
                      [:select (or (= major "CS") (= major "Games Eng"))
                       [:scan [name id {major (or (= major "CS") (= major "Games Eng"))} year]]]]
                     [:rename {course x6, sid x7, grade x8}
                      [:scan [course sid grade]]]]]
                   [:rename {grade x10, sid x11, curriculum x12, date x13}
                    [:select (or (= ?x18 sid) (and (= curriculum ?x19) (> ?x20 date)))
                     [:scan [grade sid curriculum date]]]]]]]]]]))

  (comment

    (t/testing "Row subquery"
      ;; Row subquery (won't work in execution layer, needs expression
      ;; support in table)
      (valid? "VALUES (1, 2), (SELECT x.a, x.b FROM x WHERE x.a = 10)"
              '[:apply :cross-join {subquery__1_$row$ ?subquery__1_$row$} #{}
                [:project [{subquery__1_$row$ {:a a :b b}}]
                 [:rename {x__3_a a, x__3_b b}
                  [:project [x__3_a x__3_b]
                   [:select (= x__3_a 10)
                    [:rename x__3 [:scan [{a (= a 10)} b]]]]]]]
                [:table [{:$column_1$ 1
                          :$column_2$ 2}
                         {:$column_1$ (. ?subquery__1_$row$ a)
                          :$column_2$ (. ?subquery__1_$row$ b)}]]]))))
