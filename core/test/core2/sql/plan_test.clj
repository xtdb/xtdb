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
          '[:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:select (= ms__4_birthdate 1960)
               [:rename ms__4 [:scan [name {birthdate (= birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950"
          '[:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:select (and (< ms__4_birthdate 1960) (> ms__4_birthdate 1950))
               [:rename ms__4 [:scan [name {birthdate (and (< birthdate 1960) (> birthdate 1950))}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
          '[:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:join {si__3_starName ms__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:select (and (< ms__4_birthdate 1960) (= ms__4_name "Foo"))
               [:rename ms__4 [:scan [{name (= name "Foo")} {birthdate (< birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
          '[:rename {si__3_movieTitle movieTitle}
            [:project [si__3_movieTitle]
             [:join {si__3_starName m__4_name}
              [:rename si__3 [:scan [movieTitle starName]]]
              [:rename m__4
               [:rename {ms__7_name name}
                [:project [ms__7_name]
                 [:select (= ms__7_birthdate 1960)
                  [:rename ms__7 [:scan [name {birthdate (= birthdate 1960)}]]]]]]]]]])

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
          '[:rename
            {si__3_name name si__3_lastname lastname}
            [:project
             [si__3_name si__3_lastname]
             [:select (= si__3_name si__3_lastname)
              [:rename si__3 [:scan [name lastname]]]]]])

  (valid? "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
          '[:distinct
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
          '[:difference
            [:rename
             {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename
             {si__5_name name}
             [:rename si__5 [:scan [name]]]]])


  (valid? "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
          '[:union-all
            [:rename
             {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename
             {si__5_name name}
             [:rename si__5 [:scan [name]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
          '[:intersect
            [:rename
             {si__3_name name}
             [:rename si__3 [:scan [name]]]]
            [:rename
             {si__5_name name}
             [:rename si__5 [:scan [name]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
          '[:distinct
            [:union-all
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]
             [:rename
              {si__5_name movieTitle}
              [:rename si__5 [:scan [name]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
          '[:order-by [{name :asc}]
            [:distinct
             [:union-all
              [:rename
               {si__3_name name}
               [:rename si__3 [:scan [name]]]]
              [:rename
               {si__5_name name}
               [:rename si__5 [:scan [name]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
          '[:top {:limit 10}
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
          '[:top {:skip 5}
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"
          '[:top {:skip 5 :limit 10}
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
          '[:order-by [{movieTitle :asc}]
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
          '[:top {:skip 100}
            [:order-by [{movieTitle :asc}]
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
          '[:order-by [{movieTitle :desc}]
            [:rename
             {si__3_movieTitle movieTitle}
             [:rename si__3 [:scan [movieTitle]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle"
          '[:project [movieTitle]
            [:order-by [{$order_by__1_1$ :desc} {movieTitle :asc}]
             [:project [movieTitle {$order_by__1_1$ (= si__3_year "foo")}]
              [:rename
               {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"
          '[:project [movieTitle]
            [:order-by [{$order_by__1_1$ :asc}]
             [:project [movieTitle {$order_by__1_1$ si__3_year}]
              [:rename
               {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"
          '[:order-by [{$column_1$ :asc}]
            [:project [{$column_1$ (= si__4_year "foo")}]
             [:rename si__4 [:scan [year]]]]])

  (valid? "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)"
          '[:rename
            {film__4_name name}
            [:project
             [film__4_name]
             [:unwind film__4_name
              [:project [si__3_films {film__4_name si__3_films}]
               [:rename si__3 [:scan [films]]]]]]])

  (valid? "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film"
          '[:rename
            {si__3_films films film__4_$column_1$ $column_2$}
            [:project
             [si__3_films film__4_$column_1$]
             [:unwind film__4_$column_1$
              [:project [si__3_films {film__4_$column_1$ si__3_films}]
               [:rename si__3 [:scan [films]]]]]]]))

(comment

  (t/deftest test-subqueries
    ;; Scalar subquery:
    (valid? "SELECT (1 + (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:project [{some_column (+ 1 subquery__1_$column_1$)}]
              [:cross-join
               [:select (= x__4_y 1)
                [:rename x__4 [:scan [{y (= y 1)}]]]]
               [:max-1-row
                [:rename subquery__1
                 [:project [{$column_1$ $agg_out__2_3$}]
                  [:group-by [{$agg_out__2_3$ (max $agg_in__2_3$)}]
                   [:project [{$agg_in__2_3$ foo__4_bar}]
                    [:rename foo__4 [:scan [bar]]]]]]]]]])

    ;; Correlated subquery:
    (valid? "SELECT (1 + (SELECT MAX (foo.bar + x.y) FROM foo)) AS some_column FROM x WHERE x.y = 1"
            '[:project [{some_column (+ 1 subquery__1_$column_1$)}]
              [:apply
               :cross-join
               ;; dependent column -> parameter
               {x__4_y ?x__4_y}
               ;; columns projected from the dependent relation?
               #{subquery__1_$column_1$}
               ;; independent
               [:select (= x__4_y 1)
                [:rename x__4 [:scan [{y (= y 1)}]]]]
               ;; dependent (parameterised query)
               [:max-1-row
                [:rename subquery__1
                 [:project [{$column_1$ $agg_out__2_3$}]
                  [:group-by [{$agg_out__2_3$ (max $agg_in__2_3$)}]
                   [:project [{$agg_in__2_3$ (+ foo__4_bar ?x__4_y)}]
                    [:rename foo__4 [:scan [bar]]]]]]]]]])

    ;; EXISTS
    (valid? "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:select subquery__1_$exists$
                [:apply
                 :cross-join
                 {x__2_y ?x__2_y}
                 #{}
                 [:select (= x__3_z 10)
                  [:rename x__3 [:scan [y {z (= z 10)}]]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$exists$ true}]
                    [:rename subquery__1
                     [:rename {y__3_z z}
                      [:project [y__3_z]
                       [:select (= y__3_z ?x__2_y)
                        [:rename y__3 [:scan [{z (= z ?x__2_y)}]]]]]]]]
                   [:table [{:subquery__1_$exists$ false}]]]]]]]])

    ;; NOT EXISTS
    (valid? "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
            '[:rename {x__3_y y}
              [:project [x__3_y]
               [:select subquery__1_$not_exists$
                [:apply
                 :cross-join
                 {x__2_y ?x__2_y}
                 #{}
                 [:select (= x__3_z 10)
                  [:rename x__3 [:scan [y {z (= z 10)}]]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$not_exists$ false}]
                    [:rename subquery__1
                     [:rename {y__3_z z}
                      [:project [y__3_z]
                       [:select (or (<> ?x__2_y subquery__1_z)
                                    (nil? subquery__1_z)
                                    (nil? ?x__2_y))
                        [:rename y__3 [:scan [{z (= z ?x__2_y)}]]]]]]]]
                   [:table [{:subquery__1_$not_exists$ true}]]]]]]]])

    ;; EXISTS as expression in SELECT clause:
    (valid? "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
            '[:project [{$column_1$ subquery__1_$exists$}]
              [:apply
               :cross-join
               {x__2_y ?x__2_y}
               #{}
               [:select (= x__3_z 10)
                [:rename x__3 [:scan [y {z (= z 10)}]]]]
               [:top {:limit 1}
                [:union-all
                 [:project [{subquery__1_$exists$ true}]
                  [:rename subquery__1
                   [:rename {y__3_z z}
                    [:project [y__3_z]
                     [:select (= y__3_z ?x__2_y)
                      [:rename y__3 [:scan [{z (= z ?x__2_y)}]]]]]]]]
                 [:table [{:subquery__1_$exists$ false}]]]]]])

    ;; ALL as expression in WHERE clause:
    (valid? "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
            '[:rename {x__2_y y}
              [:project [x__2_y]
               [:select subquery__1_$all$
                [:apply
                 :cross-join
                 ;; Introduced correlated column, not available in the
                 ;; scope analysis, see below.
                 {x__2_z ?x__2_z}
                 #{}
                 [:rename x__2 [:scan [y z]]]
                 ;; This outer part is like NOT EXISTS.
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$all$ false}]
                    ;; This select is ALL/ANY specific, usage of lhs,
                    ;; which most likely will contain correlated
                    ;; parameters, in this case it's just a an outer
                    ;; column reference. This expression needs to be
                    ;; postwalk-replaced as the scope analysis doesn't
                    ;; know about these introduced correlations.
                    [:select (or (<= ?x__2_z subquery__1_z)
                                 (nil? subquery__1_z)
                                 (nil? ?x__2_z))
                     [:rename subquery__1
                      [:rename {y__3_z z}
                       [:project [y__3_z]
                        [:rename y__3 [:scan [z]]]]]]]]
                   [:table [{:subquery__1_$all$ true}]]]]]]]])

    ;; ANY as expression in WHERE clause:
    (valid? "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
            '[:rename {x__2_y y}
              [:project [x__2_y]
               [:select subquery__1_$any$
                [:apply
                 :cross-join
                 {x__2_z ?x__2_z}
                 #{}
                 [:rename x__2 [:scan [y z]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$any$ true}]
                    ;; Correlated parameters are part of the original
                    ;; lhs expression.
                    [:select (> (= ?x__2_z 1) subquery__1_z)
                     [:rename subquery__1
                      [:rename {y__3_z z}
                       [:project [y__3_z]
                        [:rename y__3 [:scan [z]]]]]]]]
                   [:table [{:subquery__1_$any$ false}]]]]]]]])

    ;; IN (= ANY, EXISTS) as expression in WHERE clause:
    (valid? "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
            '[:rename {x__2_y y}
              [:project [x__2_y]
               [:select subquery__1_$in$
                [:apply
                 :cross-join
                 {x__2_z ?x__2_z}
                 #{}
                 [:rename x__2 [:scan [y z]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$in$ true}]
                    ;; Correlated parameters are part of the original
                    ;; lhs expression.
                    [:select (= ?x__2_z subquery__1_z)
                     [:rename subquery__1
                      [:rename {y__3_z z}
                       [:project [y__3_z]
                        [:rename y__3 [:scan [z]]]]]]]]
                   [:table [{:subquery__1_$in$ false}]]]]]]]])

    ;; NOT IN (<> ALL, NOT EXISTS) as expression in WHERE clause:
    (valid? "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
            '[:rename {x__2_y y}
              [:project [x__2_y]
               [:select subquery__1_$not_in$
                [:apply
                 :cross-join
                 {x__2_z ?x__2_z}
                 #{}
                 [:rename x__2 [:scan [y z]]]
                 [:top {:limit 1}
                  [:union-all
                   [:project [{subquery__1_$not_in$ false}]
                    [:select (or (<> ?x__2_z subquery__1_z)
                                 (nil? subquery__1_z)
                                 (nil? ?x__2_z))
                     [:rename subquery__1
                      [:rename {y__3_z z}
                       [:project [y__3_z]
                        [:rename y__3 [:scan [z]]]]]]]]
                   [:table [{:subquery__1_$not_in$ true}]]]]]]]])

    ;; LATERAL derived table
    (valid? "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
            '[:rename {x__3_y y, y__4_z z}
              [:project [x__3_y y__4_z]
               [:apply
                :cross-join
                {x__3_y ?x__3_y}
                #{y__4_z}
                [:rename x__3 [:scan [y]]]
                [:rename y__4
                 [:rename {z__7_z z}
                  [:project [z__7_z]
                   [:select (= z__7_z ?x__3_y)
                    [:rename z__7 [:scan [z]]]]]]]]]])

    ;; Row subquery (won't work in execution layer, needs expression
    ;; support in table)
    (valid? "VALUES (1, 2), (SELECT x.a, x.b FROM x WHERE x.a = 10)"
            '[:apply
              :cross-join
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
                        :$column_2$ (. ?subquery__1_$row$ b)}]]])))
