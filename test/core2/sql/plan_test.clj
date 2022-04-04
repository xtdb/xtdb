(ns core2.sql.plan-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]
            [core2.sql.plan :as plan]))

(defn plan-sql [sql]
  (let [tree (sql/parse sql)
        {errs :errs :as plan} (plan/plan-query tree)]
    (assert (empty? errs) errs)
    #_(select-keys plan [:fired-rules :plan]) ;; Debug Tool
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
           (plan-sql "VALUES 1, 2"))))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(t/deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (= '[:rename {x7 some_column}
               [:project [{x7 (= 1 x5)}]
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
                [:apply :anti-join {x2 ?x9} #{}
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:rename {z x4}
                  [:scan [{z (or (<= ?x9 z) (nil? ?x9) (nil? z))}]]]]]]
             (plan-sql "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"))))

  (t/testing "ANY in WHERE"
    (t/is (= '[:rename {x1 y}
               [:project [x1]
                [:apply :semi-join {x2 ?x9} #{}
                 [:rename {y x1, z x2} [:scan [y z]]]
                 [:rename {z x4}
                  [:scan [{z (> (= ?x9 1) z)}]]]]]]
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
    (t/is (= '[:rename {x1 name, x15 $column_2$}
               [:project [x1 x15]
                [:group-by [x1 x2 x3 $row_number$ {x15 (count x14)}]
                 [:map [{x14 1}]
                  [:left-outer-join [{x2 x12}]
                   [:map [{$row_number$ (row-number)}]
                    [:anti-join [{x3 x5}]
                     [:rename {name x1, custno x2, country x3}
                      [:scan [name custno country]]]
                     [:rename {country x5} [:scan [country]]]]]
                   [:rename {custno x12} [:scan [custno]]]]]]]]
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
                   [:apply :left-outer-join {x2 ?x18, x3 ?x19, x4 ?x20} #{x10 x11 x12 x13}
                    [:map [{$row_number$ (row-number)}]
                     [:join [{x2 x7}]
                      [:rename {name x1, id x2, major x3, year x4}
                       [:scan [name id {major (or (= major "CS") (= major "Games Eng"))} year]]]
                      [:rename {course x6, sid x7, grade x8}
                       [:scan [course sid grade]]]]]
                    [:rename {grade x10, sid x11, curriculum x12, date x13}
                     [:select (or (= ?x18 sid) (and (= curriculum ?x19) (> ?x20 date)))
                      [:scan [grade sid curriculum date]]]]]]]]]]
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
                       [:semi-join [{x4 x7} {x3 x6}]
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
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)"))))))

  (comment

    (t/testing "Row subquery"
      ;; Row subquery (won't work in execution layer, needs expression
      ;; support in table)
      (t/is
        (=
         '[:apply :cross-join {subquery__1_$row$ ?subquery__1_$row$} #{}
           [:project [{subquery__1_$row$ {:a a :b b}}]
            [:rename {x__3_a a, x__3_b b}
             [:project [x__3_a x__3_b]
              [:select (= x__3_a 10)
               [:rename x__3 [:scan [{a (= a 10)} b]]]]]]]
           [:table [{:$column_1$ 1
                     :$column_2$ 2}
                    {:$column_1$ (. ?subquery__1_$row$ a)
                     :$column_2$ (. ?subquery__1_$row$ b)}]]]
         (plan-sql "VALUES (1, 2), (SELECT x.a, x.b FROM x WHERE x.a = 10)"))))))
