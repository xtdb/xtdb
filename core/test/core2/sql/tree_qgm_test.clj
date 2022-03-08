(ns core2.sql.tree-qgm-test
  (:require [clojure.test :as t]
            [core2.sql.tree-qgm :as qgm]
            [core2.sql :as sql]
            [clojure.zip :as z]))

(t/deftest test-qgm
  (t/is (= {:tree '[:qgm.box/select {:qgm.box.head/distinct? false
                                     :qgm.box.body/distinct :qgm.box.body.distinct/permit
                                     :qgm.box.head/columns [price]
                                     :qgm.box.body/columns [q3__3_price]}

                    {q3__3 [:qgm.quantifier/foreach q3__3 [price partno]
                            [:qgm.box/base-table quotations]]}]

            :preds '{p4 {:qgm.predicate/expression (= q3__3_partno 1)
                         :qgm.predicate/quantifiers #{q3__3}}}}

           (-> (sql/parse "SELECT q3.price FROM quotations q3 WHERE q3.partno = 1")
               (z/vector-zip)
               (qgm/->qgm)))

        "simple query")

  (t/is (= {:tree '[:qgm.box/select {:qgm.box.head/distinct? true
                                     :qgm.box.body/distinct :qgm.box.body.distinct/enforce
                                     :qgm.box.head/columns [partno descr suppno]
                                     :qgm.box.body/columns [q1__3_partno q1__3_descr q2__4_suppno]}
                    {q1__3 [:qgm.quantifier/foreach q1__3 [partno descr]
                            [:qgm.box/base-table inventory]]

                     q2__4 [:qgm.quantifier/foreach q2__4 [suppno partno]
                            [:qgm.box/base-table quotations]]}]

            :preds '{p5 {:qgm.predicate/expression (= q1__3_partno q2__4_partno)
                         :qgm.predicate/quantifiers #{q1__3 q2__4}}

                     p6 {:qgm.predicate/expression (= q1__3_descr "engine")
                         :qgm.predicate/quantifiers #{q1__3}}}}

           (-> (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'")
               (z/vector-zip)
               (qgm/->qgm)))

        "add a join")

  (t/is (= {:tree '[:qgm.box/select {:qgm.box.head/distinct? true,
                                     :qgm.box.head/columns [partno descr suppno],
                                     :qgm.box.body/columns [q1__3_partno q1__3_descr q2__4_suppno],
                                     :qgm.box.body/distinct :qgm.box.body.distinct/enforce}

                    {q1__3 [:qgm.quantifier/foreach q1__3 [partno descr]
                            [:qgm.box/base-table inventory]]

                     q2__4 [:qgm.quantifier/foreach q2__4 [suppno partno price]
                            [:qgm.box/base-table quotations]]

                     q8 [:qgm.quantifier/all q8 [price]
                         [:qgm.box/select {:qgm.box.head/distinct? false,
                                           :qgm.box.head/columns [price],
                                           :qgm.box.body/columns [q3__9_price],
                                           :qgm.box.body/distinct :qgm.box.body.distinct/permit}
                          {q3__9 [:qgm.quantifier/foreach q3__9 [price partno]
                                  [:qgm.box/base-table quotations]]}]]}]

            :preds '{p5 {:qgm.predicate/expression (= q1__3_partno q2__4_partno),
                         :qgm.predicate/quantifiers #{q2__4 q1__3}},

                     p6 {:qgm.predicate/expression (= q1__3_descr "engine"),
                         :qgm.predicate/quantifiers #{q1__3}}

                     p10 {:qgm.predicate/expression (= q2__4_partno q3__9_partno),
                          :qgm.predicate/quantifiers #{q3__9 q2__4}}

                     hack-qp1 {:qgm.predicate/expression (<= q2__4_price q8__price)
                               :qgm.predicate/quantifiers #{q2__4 q8}}}}

           (-> (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'
  AND q2.price <= ALL (SELECT q3.price FROM quotations q3
                       WHERE q2.partno=q3.partno)")
               (z/vector-zip)
               (qgm/->qgm)))

        "all correlated sub-query")

  (t/is (= {:tree '[:qgm.box/select {:qgm.box.head/distinct? false,
                                     :qgm.box.head/columns [partno price order_qty],
                                     :qgm.box.body/columns [q1__3_partno q1__3_price q1__3_order_qty],
                                     :qgm.box.body/distinct :qgm.box.body.distinct/permit}
                    {q1__3 [:qgm.quantifier/foreach q1__3 [partno price order_qty]
                            [:qgm.box/base-table quotations]],
                     q5 [:qgm.quantifier/existential q5 [partno]
                         [:qgm.box/select {:qgm.box.head/distinct? false,
                                           :qgm.box.head/columns [partno],
                                           :qgm.box.body/columns [q3__6_partno],
                                           :qgm.box.body/distinct :qgm.box.body.distinct/permit}
                          {q3__6 [:qgm.quantifier/foreach q3__6 [partno onhand_qty type]
                                  [:qgm.box/base-table inventory]]}]]}]

            :preds '{p7 {:qgm.predicate/expression (< q3__6_onhand_qty q1__3_order_qty),
                         :qgm.predicate/quantifiers #{q3__6 q1__3}},
                     p8 {:qgm.predicate/expression (= q3__6_type "CPU"),
                         :qgm.predicate/quantifiers #{q3__6}}
                     hack-qp1 {:qgm.predicate/expression (= q1__3_partno q5__partno),
                               :qgm.predicate/quantifiers #{q1__3 q5}}}}

           (-> (sql/parse "
SELECT q1.partno, q1.price, q1.order_qty
FROM quotations q1
WHERE q1.partno IN (SELECT q3.partno
                    FROM inventory q3
                    WHERE q3.onhand_qty < q1.order_qty AND q3.type = 'CPU')")
               (z/vector-zip)
               (qgm/->qgm)))
        "existential correlated sub-query"))

(defmacro plan-is [expected-plan sql]
  `(let [sql-ast# (sql/parse ~sql)
         {errs# :errs, qgm# :qgm, plan# :plan} (qgm/plan-query sql-ast#)]
     (t/is (= [] (vec errs#)))
     (t/is (= ~expected-plan plan#))
     {:sql-ast sql-ast#
      :qgm qgm#
      :plan plan#}))

(t/deftest test-basic-queries
  (plan-is '[:rename
             {si__3_name name si__3_lastname lastname}
             [:project
              [si__3_name si__3_lastname]
              [:select (= si__3_name si__3_lastname)
               [:rename si__3 [:scan [name lastname]]]]]]
           "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname")

  (plan-is '[:distinct
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT DISTINCT si.movieTitle FROM StarsIn AS si")

  (plan-is '[:rename {q3__3_price price}
             [:project [q3__3_price]
              [:rename q3__3
               [:scan [price {partno (= partno 1)}]]]]]
           "
SELECT q3.price FROM quotations q3 WHERE q3.partno = 1")

  (plan-is '[:distinct
             [:rename {q1__3_partno partno, q1__3_descr descr, q2__4_suppno suppno}
              [:project [q1__3_partno q1__3_descr q2__4_suppno]
               [:select (= q1__3_partno q2__4_partno)
                [:cross-join
                 [:rename q1__3 [:scan [partno {descr (= descr "engine")}]]]
                 [:rename q2__4 [:scan [suppno partno]]]]]]]]
           "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'")

  #_ ; TODO
  (plan-is '[:rename {si__3_name name}
             [:rename si__3 [:scan [name]]]]
           "SELECT * FROM StarsIn AS si(name)")

  #_ ; FIXME generated col-name in projection
  (plan-is '[:order-by [{$column_1$ :asc}]
             [:project [{$column_1$ (= si__4_year "foo")}]
              [:rename si__4 [:scan [year]]]]]
           "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"))

(t/deftest test-join-plans
  (plan-is '[:rename {si__3_movieTitle movieTitle}
             [:project [si__3_movieTitle]
              [:select (= si__3_starName ms__4_name)
               [:cross-join
                [:rename si__3 [:scan [movieTitle starName]]]
                [:rename ms__4 [:scan [name {birthdate (= birthdate 1960)}]]]]]]]
           #_ ; TODO cross-join -> join
           '[:rename
             {si__3_movieTitle movieTitle}
             [:project
              [si__3_movieTitle]
              [:join {si__3_starName ms__4_name}
               [:rename si__3 [:scan [movieTitle starName]]]
               [:rename ms__4 [:scan [name {birthdate (= birthdate 1960)}]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960")

  (plan-is '[:rename {si__3_movieTitle movieTitle}
             [:project [si__3_movieTitle]
              [:select (= si__3_starName ms__4_name)
               [:cross-join
                [:rename si__3 [:scan [movieTitle starName]]]
                [:rename ms__4 [:scan [name
                                       {birthdate (and (< birthdate 1960)
                                                       (> birthdate 1950))}]]]]]]]
           #_ ; TODO cross-join -> join
           '[:rename
             {si__3_movieTitle movieTitle}
             [:project
              [si__3_movieTitle]
              [:join {si__3_starName ms__4_name}
               [:rename si__3 [:scan [movieTitle starName]]]
               [:rename ms__4 [:scan [name {birthdate (and (< birthdate 1960)
                                                           (> birthdate 1950))}]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950")

  (plan-is '[:rename {si__3_movieTitle movieTitle}
             [:project [si__3_movieTitle]
              [:select (= si__3_starName ms__4_name)
               [:cross-join
                [:rename si__3 [:scan [movieTitle starName]]]
                [:rename ms__4
                 [:scan [{name (= name "Foo")}
                         {birthdate (< birthdate 1960)}]]]]]]]

           #_ ; TODO cross-join -> join
           '[:rename {si__3_movieTitle movieTitle}
             [:project [si__3_movieTitle]
              [:join {si__3_starName ms__4_name}
               [:rename si__3 [:scan [movieTitle starName]]]
               [:rename ms__4 [:scan [{name (= name "Foo")}
                                      {birthdate (< birthdate 1960)}]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'")

  (plan-is '[:rename {si__4_movieTitle movieTitle}
             [:project [si__4_movieTitle]
              [:select (and (= si__4_year m__3_movieYear)
                            (= m__3_title si__4_movieTitle))
               [:cross-join
                [:rename m__3 [:scan [title movieYear]]]
                [:rename si__4 [:scan [movieTitle year]]]]]]]

           #_ ; TODO cross-join -> join
           '[:rename {si__4_movieTitle movieTitle}
             [:project [si__4_movieTitle]
              [:select (= si__4_year m__3_movieYear)
               [:join {m__3_title si__4_movieTitle}
                [:rename m__3 [:scan [title movieYear]]]
                [:rename si__4 [:scan [movieTitle year]]]]]]]
           "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")

  (plan-is '[:rename {si__4_title title}
             [:project [si__4_title]
              [:select (= m__3_title si__4_title)
               [:cross-join
                [:rename m__3 [:scan [title]]]
                [:rename si__4 [:scan [title]]]]]]]

           #_ ; TODO cross-join -> join
           '[:rename {si__4_title title}
             [:project [si__4_title]
              [:join {m__3_title si__4_title}
               [:rename m__3 [:scan [title]]]
               [:rename si__4 [:scan [title]]]]]]
           "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)")

  (t/testing "outer joins"
    #_
    (plan-is '[:rename {si__4_movieTitle movieTitle}
               [:project [si__4_movieTitle]
                [:select (= si__4_year m__3_movieYear)
                 [:left-outer-join {m__3_title si__4_movieTitle}
                  [:rename m__3 [:scan [title movieYear]]]
                  [:rename si__4 [:scan [movieTitle year]]]]]]]
             "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear")

    #_
    (plan-is '[:rename {si__4_title title}
               [:project [si__4_title]
                [:left-outer-join {si__4_title m__3_title}
                 [:rename si__4 [:scan [title]]]
                 [:rename m__3 [:scan [title]]]]]]
             "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)")))


(t/deftest test-from-subquery
  #_
  (plan-is '[:rename {foo__3_bar bar}
             [:project [foo__3_bar]
              [:rename foo__3
               [:rename {si__6_name bar}
                [:rename si__6 [:scan [name]]]]]]]
           "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)")

  #_
  (plan-is '[:rename {si__3_movieTitle movieTitle}
             [:project [si__3_movieTitle]
              [:join {si__3_starName m__4_name}
               [:rename si__3 [:scan [movieTitle starName]]]
               [:rename m__4
                [:rename {ms__7_name name}
                 [:project [ms__7_name]
                  [:select (= ms__7_birthdate 1960)
                   [:rename ms__7 [:scan [name {birthdate (= birthdate 1960)}]]]]]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"))


(t/deftest test-grouping-plans
  #_
  (plan-is '[:rename {me__4_name name}
             [:project [me__4_name {$column_2$ $agg_out__2_3$}]
              [:select (< $agg_out__2_8$ 1930)
               [:group-by [me__4_name
                           {$agg_out__2_3$ (sum $agg_in__2_3$)}
                           {$agg_out__2_8$ (min $agg_in__2_8$)}]
                [:project [me__4_name {$agg_in__2_3$ m__5_length} {$agg_in__2_8$ m__5_year}]
                 [:join {me__4_cert m__5_producer}
                  [:rename me__4 [:scan [name cert]]]
                  [:rename m__5 [:scan [length producer year]]]]]]]]]
           "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930")

  #_
  (plan-is '[:project [{$column_1$ $agg_out__2_3$}]
             [:group-by [{$agg_out__2_3$ (sum $agg_in__2_3$)}]
              [:project [{$agg_in__2_3$ m__4_length}]
               [:rename m__4 [:scan [length]]]]]]
           "SELECT SUM(m.length) FROM Movie AS m"))


(t/deftest test-set-plans
  (plan-is '[:difference
             [:rename
              {si__3_name name}
              [:rename si__3 [:scan [name]]]]
             [:rename
              {si__5_name name}
              [:rename si__5 [:scan [name]]]]]
           "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si")

  (plan-is '[:union-all
             [:rename
              {si__3_name name}
              [:rename si__3 [:scan [name]]]]
             [:rename
              {si__5_name name}
              [:rename si__5 [:scan [name]]]]]
           "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si")

  (plan-is '[:intersection
             [:rename
              {si__3_name name}
              [:rename si__3 [:scan [name]]]]
             [:rename
              {si__5_name name}
              [:rename si__5 [:scan [name]]]]]
           "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si")

  (plan-is '[:distinct
             [:union-all
              [:rename
               {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle]]]]
              [:rename
               {si__5_name name}
               [:rename si__5 [:scan [name]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si")

  (plan-is '[:order-by [{name :asc}]
             [:distinct
              [:union-all
               [:rename
                {si__3_name name}
                [:rename si__3 [:scan [name]]]]
               [:rename
                {si__5_name name}
                [:rename si__5 [:scan [name]]]]]]]
           "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"))


(t/deftest test-order-limit-offset-plans
  (plan-is '[:top {:limit 10}
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY")

  (plan-is '[:top {:skip 5}
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS")

  (plan-is '[:top {:skip 5 :limit 10}
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY")

  (plan-is '[:order-by [{movieTitle :asc}]
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle")

  (plan-is '[:top {:skip 100}
             [:order-by [{movieTitle :asc}]
              [:rename
               {si__3_movieTitle movieTitle}
               [:rename si__3 [:scan [movieTitle]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS")

  (plan-is '[:order-by [{movieTitle :desc}]
             [:rename
              {si__3_movieTitle movieTitle}
              [:rename si__3 [:scan [movieTitle]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC")

  (plan-is '[:project [movieTitle]
             [:order-by [{$order_by__1_1$ :desc} {movieTitle :asc}]
              [:project [movieTitle {$order_by__1_1$ (= si__3_year "foo")}]
               [:rename
                {si__3_movieTitle movieTitle}
                [:rename si__3 [:scan [movieTitle year]]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle")

  (plan-is '[:project [movieTitle]
             [:order-by [{$order_by__1_1$ :asc}]
              [:project [movieTitle {$order_by__1_1$ si__3_year}]
               [:rename
                {si__3_movieTitle movieTitle}
                [:rename si__3 [:scan [movieTitle year]]]]]]]
           "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"))

(t/deftest test-unnest
  #_
  (plan-is '[:rename
             {film__4_name name}
             [:project
              [film__4_name]
              [:unwind film__4_name
               [:project [si__3_films {film__4_name si__3_films}]
                [:rename si__3 [:scan [films]]]]]]]
           "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)")

  #_
  (plan-is '[:rename
             {si__3_films films film__4_$column_1$ $column_2$}
             [:project
              [si__3_films film__4_$column_1$]
              [:unwind film__4_$column_1$
               [:project [si__3_films {film__4_$column_1$ si__3_films}]
                [:rename si__3 [:scan [films]]]]]]]
           "SELECT * FROM StarsIn AS si, UNNEST(si.films) AS film"))
