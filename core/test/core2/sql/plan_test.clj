(ns core2.sql.plan-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(defmacro valid? [sql expected]
  `(let [tree# (sql/parse ~sql)
         {errs# :errs plan# :plan} (sql/plan-query tree#)]
     (t/is (empty? errs#))
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
             [:select (and (> ms__4_birthdate 1950) (< ms__4_birthdate 1960))
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
             [:rename m__3 [:scan [title]]]]]]))
