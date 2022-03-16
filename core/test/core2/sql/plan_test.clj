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
          '[:rename {si__3_movieTitle movieTitle}
            [:project [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:rename ms__4
               [:select (= birthdate 1960)
                [:scan [name {birthdate (= birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950"
          '[:rename {si__3_movieTitle movieTitle}
            [:project [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:rename ms__4
               [:select (and (< birthdate 1960) (> birthdate 1950))
                [:scan [name {birthdate (and (< birthdate 1960) (> birthdate 1950))}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
          '[:rename {si__3_movieTitle movieTitle}
            [:project [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:rename ms__4
               [:select (and (< birthdate 1960) (= name "Foo"))
                [:scan [{name (= name "Foo")} {birthdate (< birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
          '[:rename {si__3_movieTitle movieTitle}
            [:project [si__3_movieTitle]
             [:join {si__3_starName m__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:rename m__4
               [:rename {ms__7_name name}
                [:project [ms__7_name]
                 [:rename ms__7
                  [:select (= birthdate 1960)
                   [:scan [name {birthdate (= birthdate 1960)}]]]]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          '[:rename {si__4_movieTitle movieTitle}
            [:project [si__4_movieTitle]
             [:select (= si__4_year m__3_movieYear)
              [:join {m__3_title si__4_movieTitle}
               [:rename m__3 [:scan [title movieYear]]]
               [:rename si__4 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          '[:rename {si__4_movieTitle movieTitle}
            [:project [si__4_movieTitle]
             [:select (= si__4_year m__3_movieYear)
              [:left-outer-join {m__3_title si__4_movieTitle}
               [:rename m__3 [:scan [title movieYear]]]
               [:rename si__4 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)"
          '[:rename {si__4_title title}
            [:project [si__4_title]
             [:join {m__3_title si__4_title}
              [:rename m__3 [:scan [title]]]
              [:rename si__4 [:scan [title]]]]]])

  (valid? "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)"
          '[:rename {si__4_title title}
            [:project [si__4_title]
             [:left-outer-join {si__4_title m__3_title}
              [:rename si__4 [:scan [title]]]
              [:rename m__3 [:scan [title]]]]]])

  (valid? "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930"
          '[:rename {me__4_name name}
            [:project [me__4_name {$column_2$ $agg_out__2_3$}]
             [:select (< $agg_out__2_8$ 1930)
              [:group-by [me__4_name
                          {$agg_out__2_3$ (sum $agg_in__2_3$)}
                          {$agg_out__2_8$ (min $agg_in__2_8$)}]
               [:project [me__4_name {$agg_in__2_3$ m__5_length} {$agg_in__2_8$ m__5_year}]
                [:join {me__4_cert m__5_producer}
                 [:rename me__4 [:scan [name cert]]]
                 [:rename m__5 [:scan [length producer year]]]]]]]]])

  (valid? "SELECT SUM(m.length) FROM Movie AS m"
          '[:project [{$column_1$ $agg_out__2_3$}]
            [:group-by [{$agg_out__2_3$ (sum $agg_in__2_3$)}]
             [:project [{$agg_in__2_3$ m__4_length}]
              [:rename m__4 [:scan [length]]]]]])

  (valid? "SELECT * FROM StarsIn AS si(name)"
          '[:rename
            {si__3_name name}
            [:rename si__3 [:scan [name]]]])

  (valid? "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
          '[:rename {foo__3_bar bar}
            [:project [foo__3_bar]
             [:rename foo__3
              [:rename {si__6_name bar}
               [:rename si__6 [:scan [name]]]]]]])

  (valid? "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname"
          '[:rename {si__3_name name si__3_lastname lastname}
            [:project [si__3_name si__3_lastname]
             [:rename si__3
              [:select (= name lastname)
               [:scan [name lastname]]]]]])

  (valid? "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
          '[:distinct
            [:rename {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
          '[:difference
            [:rename {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename {si__5_name name}
             [:rename si__5 [:scan [name]]]]])


  (valid? "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
          '[:union-all
            [:rename {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename {si__5_name name}
             [:rename si__5 [:scan [name]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
          '[:intersect
            [:rename {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename {si__5_name name}
             [:rename si__5 [:scan [name]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
          '[:distinct
            [:union-all
             [:rename {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]
             [:rename {si__5_name movieTitle}
              [:rename si__5 [:scan [name]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
          '[:order-by [{name :asc}]
            [:distinct
             [:union-all
              [:rename {si__3_name name}
               [:rename si__3 [:scan [name]]]]
              [:rename {si__5_name name}
               [:rename si__5 [:scan [name]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
          '[:top {:limit 10}
            [:rename {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
          '[:top {:skip 5}
            [:rename {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"
          '[:top {:skip 5 :limit 10}
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
          '[:order-by [{movieTitle :asc}]
            [:rename {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
          '[:top {:skip 100}
            [:order-by [{movieTitle :asc}]
             [:rename {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
          '[:order-by [{movieTitle :desc}]
            [:rename {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle"
          '[:project [movieTitle]
            [:order-by [{$order_by__1_1$ :desc} {movieTitle :asc}]
             [:project [movieTitle {$order_by__1_1$ (= si__3_year "foo")}]
              [:rename {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"
          '[:project [movieTitle]
            [:order-by [{$order_by__1_1$ :asc}]
             [:project [movieTitle {$order_by__1_1$ si__3_year}]
              [:rename {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"
          '[:order-by [{$column_1$ :asc}]
            [:project [{$column_1$ (= si__4_year "foo")}]
             [:rename si__4 [:scan [year]]]]])

  (valid? "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)"
          '[:rename {film__4_name name}
            [:project [film__4_name]
             [:unwind {film__4_name film__4_name} {}
              [:project [si__3_films {film__4_name si__3_films}]
               [:rename si__3 [:scan [films]]]]]]])

  (valid? "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film"
          '[:rename {si__3_films films film__4_$column_1$ $column_2$}
            [:project [si__3_films film__4_$column_1$]
             [:unwind {film__4_$column_1$ film__4_$column_1$} {}
              [:project [si__3_films {film__4_$column_1$ si__3_films}]
               [:rename si__3 [:scan [films]]]]]]])

  (valid? "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
          '[:rename {si__3_films films film__4_$column_1$ $column_2$ film__4_$column_2$ $column_3$}
            [:project [si__3_films film__4_$column_1$ film__4_$column_2$]
             [:unwind {film__4_$column_1$ film__4_$column_1$} {:ordinality-column film__4_$column_2$}
              [:project [si__3_films {film__4_$column_1$ si__3_films}]
               [:rename si__3 [:scan [films]]]]]]]))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(t/deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (valid? "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:project [{some_column (= 1 subquery__4_$column_1$)}]
              [:cross-join
               [:rename x__8
                [:select (= y 1)
                 [:scan [{y (= y 1)}]]]]
               [:max-1-row
                [:rename subquery__4
                 [:project [{$column_1$ $agg_out__5_6$}]
                  [:group-by [{$agg_out__5_6$ (max $agg_in__5_6$)}]
                   [:project [{$agg_in__5_6$ foo__7_bar}]
                    [:rename foo__7 [:scan [bar]]]]]]]]]]))

  (t/testing "Scalar subquery in WHERE"
    (valid? "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"
            '[:rename {x__3_y some_column}
              [:project [x__3_y]
               [:join {x__3_y subquery__5_$column_1$}
                [:rename x__3 [:scan [y]]]
                [:max-1-row
                 [:rename subquery__5
                  [:project [{$column_1$ $agg_out__6_7$}]
                   [:group-by [{$agg_out__6_7$ (max $agg_in__6_7$)}]
                    [:project [{$agg_in__6_7$ foo__8_bar}]
                     [:rename foo__8 [:scan [bar]]]]]]]]]]]))

  (t/testing "Correlated scalar subquery in SELECT"
    (valid? "SELECT (1 = (SELECT foo.bar = x.y FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:project [{some_column (= 1 subquery__4_$column_1$)}]
              [:apply :cross-join {x__8_y ?x__8_y} #{subquery__4_$column_1$}
               [:rename x__8
                [:select (= y 1)
                 [:scan [{y (= y 1)}]]]]
               [:max-1-row
                [:rename subquery__4
                 [:project
                  [{$column_1$ (= foo__7_bar ?x__8_y)}]
                  [:rename foo__7 [:scan [bar]]]]]]]]))

  (t/testing "EXISTS in WHERE"
    (valid? "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:semi-join {x__3_y subquery__4_z}
                [:rename x__3 [:select (= z 10) [:scan [y {z (= z 10)}]]]]
                [:rename subquery__4
                 [:rename {y__6_z z}
                  [:rename y__6 [:scan [z]]]]]]]]))

  (t/testing "EXISTS as expression in SELECT"
    (valid? "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
            '[:project [{$column_1$ subquery__3_$exists$}]
              [:apply :cross-join {x__7_y ?x__7_y} #{subquery__3_$exists$}
               [:rename x__7
                [:select (= z 10)
                 [:scan [y {z (= z 10)}]]]]
               [:top {:limit 1}
                [:union-all
                 [:project [{subquery__3_$exists$ true}]
                  [:rename subquery__3
                   [:rename {y__5_z z}
                    [:rename y__5
                     [:select (= z ?x__7_y)
                      [:scan [{z (= z ?x__7_y)}]]]]]]]
                 [:table [{:subquery__3_$exists$ false}]]]]]]))

  (t/testing "NOT EXISTS in WHERE"
    (valid? "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:anti-join {x__3_y subquery__4_z}
                [:rename x__3
                 [:select (= z 10)
                  [:scan [y {z (= z 10)}]]]]
                [:rename subquery__4
                 [:rename {y__6_z z}
                  [:rename y__6 [:scan [z]]]]]]]]))

  (t/testing "IN in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:semi-join {x__3_z subquery__4_z}
                [:rename x__3 [:scan [y z]]]
                [:rename subquery__4
                 [:rename {y__6_z z}
                  [:rename y__6
                   [:scan [z]]]]]]]]))

  (t/testing "NOT IN in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:anti-join {x__3_z subquery__4_z}
                [:rename x__3 [:scan [y z]]]
                [:rename subquery__4
                 [:rename {y__6_z z}
                  [:rename y__6
                   [:scan [z]]]]]]]]))

  (t/testing "ALL in WHERE"
    (valid? "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:apply :anti-join {x__3_z ?x__3_z} #{}
                [:rename x__3 [:scan [y z]]]
                [:rename subquery__4
                 [:rename {y__6_z z}
                  [:rename y__6
                   [:select (or (<= ?x__3_z z)
                                (nil? ?x__3_z)
                                (nil? z))
                    [:scan [{z (or (<= ?x__3_z z)
                                   (nil? ?x__3_z)
                                   (nil? z))}]]]]]]]]]))

  (t/testing "ANY in WHERE"
    (valid? "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:apply :semi-join {x__3_z ?x__3_z} #{}
                [:rename x__3 [:scan [y z]]]
                [:rename subquery__5
                 [:rename {y__7_z z}
                  [:rename y__7
                   [:select (> (= ?x__3_z 1) z)
                    [:scan [{z (> (= ?x__3_z 1) z)}]]]]]]]]]))

  (t/testing "ALL as expression in SELECT"
    (valid? "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"
            '[:project [{$column_1$ (not subquery__3_$exists$)}]
              [:apply :cross-join {x__6_z ?x__6_z} #{subquery__3_$exists$}
               [:rename x__6 [:scan [z]]]
               [:top {:limit 1}
                [:union-all
                 [:project [{subquery__3_$exists$ true}]
                  [:rename subquery__3
                   [:rename {y__5_z z}
                    [:rename y__5
                     [:select (or (> ?x__6_z z)
                                  (nil? ?x__6_z)
                                  (nil? z))
                      [:scan [{z (or (> ?x__6_z z)
                                     (nil? ?x__6_z)
                                     (nil? z))}]]]]]]]
                 [:table [{:subquery__3_$exists$ false}]]]]]]))

  (t/testing "LATERAL derived table"
    (valid? "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
            '[:rename {x__3_y y, y__4_z z}
              [:project [x__3_y y__4_z]
               [:join {x__3_y y__4_z}
                [:rename x__3 [:scan [y]]]
                [:rename y__4
                 [:rename {z__7_z z}
                  [:rename z__7 [:scan [z]]]]]]]])

    (valid? "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"
            '[:rename {y__3_z z}
              [:project [y__3_z]
               [:rename y__3
                [:rename {z__6_z z}
                 [:project [z__6_z]
                  [:rename z__6
                   [:select (= z 1)
                    [:scan [{z (= z 1)}]]]]]]]]]))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (valid? "SELECT c.custkey FROM customer c
             WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)"
            '[:rename {c__3_custkey custkey}
              [:project [c__3_custkey]
               [:rename {$column_1$ subquery__5_$column_1$}
                [:select (< 1000000 $column_1$)
                 [:project [c__3_custkey {$column_1$ $agg_out__6_7$}]
                  [:group-by [c__3_custkey $row_number$ {$agg_out__6_7$ (sum $agg_in__6_7$)}]
                   [:project [c__3_custkey $row_number$ {$agg_in__6_7$ o__8_totalprice}]
                    [:left-outer-join {c__3_custkey o__8_custkey}
                     [:project [c__3_custkey {$row_number$ (row_number)}]
                      [:rename c__3 [:scan [custkey]]]]
                     [:rename o__8 [:scan [totalprice custkey]]]]]]]]]]])

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (valid? "SELECT * FROM customers
             WHERE customers.country = 'Mexico' AND
                   EXISTS (SELECT * FROM orders WHERE customers.custno = orders.custno)"
            '[:rename {customers__3_country country, customers__3_custno custno}
              [:project [customers__3_country customers__3_custno]
               [:semi-join {customers__3_custno subquery__5_custno}
                [:rename customers__3
                 [:select (= country "Mexico")
                  [:scan [{country (= country "Mexico")} custno]]]]
                [:rename subquery__5
                 [:rename {orders__7_custno custno}
                  [:rename orders__7 [:scan [custno]]]]]]]])

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (valid? "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)"
            '[:rename {customers__8_name name}
              [:project [customers__8_name {$column_2$ subquery__3_$column_1$}]
               [:rename {$column_1$ subquery__3_$column_1$}
                [:project [customers__8_name customers__8_custno customers__8_country {$column_1$ $agg_out__4_5$}]
                 [:group-by [customers__8_name customers__8_custno customers__8_country $row_number$ {$agg_out__4_5$ (count $agg_in__4_5$)}]
                  [:project [customers__8_name customers__8_custno customers__8_country $row_number$ {$agg_in__4_5$ 1}]
                   [:left-outer-join {customers__8_custno orders__6_custno}
                    [:project [customers__8_name customers__8_custno customers__8_country {$row_number$ (row_number)}]
                     [:anti-join {customers__8_country subquery__9_country}
                      [:rename customers__8 [:scan [name custno country]]]
                      [:rename subquery__9
                       [:rename {salesp__11_country country}
                        [:rename salesp__11 [:scan [country]]]]]]]
                    [:rename orders__6 [:scan [custno]]]]]]]]]])

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (valid? "SELECT s.name, e.course
             FROM students s, exams e
             WHERE s.id = e.sid AND
             e.grade = (SELECT MIN(e2.grade)
                        FROM exams e2
                        WHERE s.id = e2.sid)"
            '[:rename {s__3_name name, e__4_course course}
              [:project [s__3_name e__4_course]
               [:rename {$column_1$ subquery__7_$column_1$}
                [:select (= e__4_grade $column_1$)
                 [:project [s__3_name s__3_id e__4_course e__4_sid e__4_grade {$column_1$ $agg_out__8_9$}]
                  [:group-by [s__3_name s__3_id e__4_course e__4_sid e__4_grade $row_number$ {$agg_out__8_9$ (min $agg_in__8_9$)}]
                   [:project [s__3_name s__3_id e__4_course e__4_sid e__4_grade $row_number$ {$agg_in__8_9$ e2__10_grade}]
                    [:left-outer-join {s__3_id e2__10_sid}
                     [:project [s__3_name s__3_id e__4_course e__4_sid e__4_grade {$row_number$ (row_number)}]
                      [:join {s__3_id e__4_sid}
                       [:rename s__3 [:scan [name id]]]
                       [:rename e__4 [:scan [course sid grade]]]]]
                     [:rename e2__10 [:scan [grade sid]]]]]]]]]]])

    (valid? "SELECT s.name, e.course
             FROM students s, exams e
             WHERE s.id = e.sid AND
                   (s.major = 'CS' OR s.major = 'Games Eng') AND
                   e.grade >= (SELECT AVG(e2.grade) + 1
                               FROM exams e2
                               WHERE s.id = e2.sid OR
                                     (e2.curriculum = s.major AND
                                      s.year > e2.date))"
            '[:rename {s__3_name name, e__4_course course}
              [:project [s__3_name e__4_course]
               [:rename {$column_1$ subquery__9_$column_1$}
                [:select (>= e__4_grade $column_1$)
                 [:project [s__3_name s__3_id s__3_major s__3_year e__4_course e__4_sid e__4_grade {$column_1$ (+ $agg_out__10_11$ 1)}]
                  [:group-by [s__3_name s__3_id s__3_major s__3_year e__4_course e__4_sid e__4_grade $row_number$ {$agg_out__10_11$ (avg $agg_in__10_11$)}]
                   [:project [s__3_name s__3_id s__3_major s__3_year e__4_course e__4_sid e__4_grade $row_number$ {$agg_in__10_11$ e2__12_grade}]
                    [:apply :left-outer-join {s__3_id ?s__3_id, s__3_major ?s__3_major, s__3_year ?s__3_year} #{subquery__9_$column_1$}
                     [:project [s__3_name s__3_id s__3_major s__3_year e__4_course e__4_sid e__4_grade {$row_number$ (row_number)}]
                      [:join {s__3_id e__4_sid}
                       [:rename s__3
                        [:select (or (= major "CS") (= major "Games Eng"))
                         [:scan [name id {major (or (= major "CS") (= major "Games Eng"))} year]]]]
                       [:rename e__4 [:scan [course sid grade]]]]]
                     [:rename e2__12
                      [:select (or (= ?s__3_id sid) (and (= curriculum ?s__3_major) (> ?s__3_year date)))
                       [:scan [grade sid curriculum date]]]]]]]]]]]]))

  (comment

    (t/testing "Row subquery"
      ;; Row subquery (won't work in execution layer, needs expression
      ;; support in table)
      (valid? "VALUES (1, 2), (SELECT x.a, x.b FROM x WHERE x.a = 10)"
              '[:apply :cross-join
                {subquery__1_$row$ ?subquery__1_$row$}
                #{}
                [:project [{subquery__1_$row$ {:a a :b b}}]
                 [:rename {x__3_a a, x__3_b b}
                  [:project [x__3_a x__3_b]
                   [:select (= x__3_a 10)
                    [:rename x__3 [:scan [{a (= a 10)} b]]]]]]]
                [:table [{:$column_1$ 1
                          :$column_2$ 2}
                         {:$column_1$ (. ?subquery__1_$row$ a)
                          :$column_2$ (. ?subquery__1_$row$ b)}]]]))))
