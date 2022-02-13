(ns core2.sql.plan-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(defmacro valid? [sql expected]
  `(let [tree# (sql/parse ~sql)
         {errs# :errs plan# :plan} (sql/plan-query tree#)]
     (t/is (= [] (vec errs#)))
     (t/is (= '~expected plan#))
     {:tree tree# :plan plan#}))

(t/deftest test-basic-queries
  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960"
          [:rename
           {si__3_movieTitle movieTitle}
           [:project
            [si__3_movieTitle]
            [:join {si__3_starName ms__4_name}
             [:rename si__3 [:scan [movieTitle starName]]]
             [:select (= ms__4_birthdate 1960)
              [:rename ms__4 [:scan [name {birthdate (= birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950"
          [:rename
           {si__3_movieTitle movieTitle}
           [:project
            [si__3_movieTitle]
            [:join {si__3_starName ms__4_name}
             [:rename si__3 [:scan [movieTitle starName]]]
             [:select (and (< ms__4_birthdate 1960) (> ms__4_birthdate 1950))
              [:rename ms__4 [:scan [name {birthdate (and (< birthdate 1960) (> birthdate 1950))}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
          [:rename
           {si__3_movieTitle movieTitle}
           [:project
            [si__3_movieTitle]
            [:join {si__3_starName ms__4_name}
             [:rename si__3 [:scan [movieTitle starName]]]
             [:select (and (< ms__4_birthdate 1960) (= ms__4_name "Foo"))
              [:rename ms__4 [:scan [{name (= name "Foo")} {birthdate (< birthdate 1960)}]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
          [:rename {si__3_movieTitle movieTitle}
           [:project [si__3_movieTitle]
            [:join {si__3_starName m__4_name}
             [:rename si__3 [:scan [movieTitle starName]]]
             [:rename m__4
              [:rename {ms__7_name name}
               [:project [ms__7_name]
                [:select (= ms__7_birthdate 1960)
                 [:rename ms__7 [:scan [name {birthdate (= birthdate 1960)}]]]]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          [:rename {si__4_movieTitle movieTitle}
           [:project [si__4_movieTitle]
            [:select (= si__4_year m__3_movieYear)
             [:join {m__3_title si__4_movieTitle}
              [:rename m__3 [:scan [title movieYear]]]
              [:rename si__4 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
          [:rename {si__4_movieTitle movieTitle}
           [:project [si__4_movieTitle]
            [:select (= si__4_year m__3_movieYear)
             [:left-outer-join {m__3_title si__4_movieTitle}
              [:rename m__3 [:scan [title movieYear]]]
              [:rename si__4 [:scan [movieTitle year]]]]]]])

  (valid? "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)"
          [:rename {si__4_title title}
           [:project [si__4_title]
            [:join {m__3_title si__4_title}
             [:rename m__3 [:scan [title]]]
             [:rename si__4 [:scan [title]]]]]])

  (valid? "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)"
          [:rename {si__4_title title}
           [:project [si__4_title]
            [:left-outer-join {si__4_title m__3_title}
             [:rename si__4 [:scan [title]]]
             [:rename m__3 [:scan [title]]]]]])

  (valid? "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930"
          [:rename {me__4_name name}
           [:project [me__4_name {$column_2$ $agg_out__2_3$}]
            [:select (< $agg_out__2_6$ 1930)
             [:group-by [me__4_name {$agg_out__2_3$ (sum $agg_in__2_3$)} {$agg_out__2_6$ (min $agg_in__2_6$)}]
              [:project [me__4_name {$agg_in__2_3$ m__5_length} {$agg_in__2_6$ m__5_year}]
               [:join {me__4_cert m__5_producer}
                [:rename me__4 [:scan [name cert]]]
                [:rename m__5 [:scan [length producer year]]]]]]]]])

  (valid? "SELECT SUM(m.length) FROM Movie AS m"
          [:project [{$column_1$ $agg_out__2_3$}]
           [:group-by [{$agg_out__2_3$ (sum $agg_in__2_3$)}]
            [:project [{$agg_in__2_3$ m__4_length}]
             [:rename m__4 [:scan [length]]]]]])

  (valid? "SELECT * FROM StarsIn AS si(name)"
          [:rename
           {si__3_name name}
           [:project
            [si__3_name]
            [:rename si__3 [:scan [name]]]]])

  (valid? "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname"
          [:rename
           {si__3_name name si__3_lastname lastname}
           [:project
            [si__3_name si__3_lastname]
            [:select (= si__3_name si__3_lastname)
             [:rename si__3 [:scan [name lastname]]]]]])

  (valid? "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
          [:distinct
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
          [:difference
           [:rename
            {si__3_name name}
            [:project
             [si__3_name]
             [:rename si__3 [:scan [name]]]]]
           [:rename
            {si__5_name name}
            [:project
             [si__5_name]
             [:rename si__5 [:scan [name]]]]]])


  (valid? "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
          [:union-all
           [:rename
            {si__3_name name}
            [:project
             [si__3_name]
             [:rename si__3 [:scan [name]]]]]
           [:rename
            {si__5_name name}
            [:project
             [si__5_name]
             [:rename si__5 [:scan [name]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
          [:intersect
           [:rename
            {si__3_name name}
            [:project
             [si__3_name]
             [:rename si__3 [:scan [name]]]]]
           [:rename
            {si__5_name name}
            [:project
             [si__5_name]
             [:rename si__5 [:scan [name]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
          [:distinct
           [:union-all
            [:rename
             {si__3_movieTitle movieTitle}
             [:project
              [si__3_movieTitle]
              [:rename si__3 [:scan [movieTitle]]]]]
            [:rename
             {name movieTitle}
             [:rename
              {si__5_name name}
              [:project
               [si__5_name]
               [:rename si__5 [:scan [name]]]]]]]])

  (valid? "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
          [:order-by [{name :asc}]
           [:distinct
            [:union-all
             [:rename
              {si__3_name name}
              [:project
               [si__3_name]
               [:rename si__3 [:scan [name]]]]]
             [:rename
              {si__5_name name}
              [:project
               [si__5_name]
               [:rename si__5 [:scan [name]]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
          [:top {:limit 10}
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
          [:top {:skip 5}
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY"
          [:top {:skip 5 :limit 10}
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
          [:order-by [{movieTitle :asc}]
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
          [:top {:skip 100}
           [:order-by [{movieTitle :asc}]
            [:rename
             {si__3_movieTitle movieTitle}
             [:project
              [si__3_movieTitle]
              [:rename si__3 [:scan [movieTitle]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
          [:order-by [{movieTitle :desc}]
           [:rename
            {si__3_movieTitle movieTitle}
            [:project
             [si__3_movieTitle]
             [:rename si__3 [:scan [movieTitle]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle"
          [:project [movieTitle]
           [:order-by [{$order_by__1_1$ :desc} {movieTitle :asc}]
            [:project [movieTitle {$order_by__1_1$ (= si__3_year "foo")}]
             [:rename
              {si__3_movieTitle movieTitle}
              [:project
               [si__3_movieTitle si__3_year]
               [:rename si__3 [:scan [movieTitle year]]]]]]]])

  (valid? "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"
          [:project [movieTitle]
           [:order-by [{$order_by__1_1$ :asc}]
            [:project [movieTitle {$order_by__1_1$ si__3_year}]
             [:rename
              {si__3_movieTitle movieTitle}
              [:project
               [si__3_movieTitle si__3_year]
               [:rename si__3 [:scan [movieTitle year]]]]]]]])

  (valid? "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"
          [:order-by [{$column_1$ :asc}]
           [:project [{$column_1$ (= si__3_year "foo")}]
            [:rename si__3 [:scan [year]]]]]))
