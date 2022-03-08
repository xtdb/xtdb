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
               (qgm/qgm)))

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
               (qgm/qgm)))

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
               (qgm/qgm)))

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
               (qgm/qgm)))
        "existential correlated sub-query"))

(t/deftest test-qgm-query-plan
  (t/is (= {:plan '[:rename {q3__3_price price}
                    [:project [q3__3_price]
                     [:select (= q3__3_partno 1)
                      [:rename q3__3
                       [:scan [price {partno (= partno 1)}]]]]]]}
           (qgm/plan-query (sql/parse "
SELECT q3.price FROM quotations q3 WHERE q3.partno = 1"))))

  (t/is (= {:plan '[:distinct
                    [:rename {q1__3_partno partno, q1__3_descr descr, q2__4_suppno suppno}
                     [:project [q1__3_partno q1__3_descr q2__4_suppno]
                      [:select (and (= q1__3_descr "engine")
                                    (= q1__3_partno q2__4_partno))
                       [:cross-join
                        [:rename q1__3 [:scan [partno descr]]]
                        [:rename q2__4 [:scan [suppno partno]]]]]]]]}
           (qgm/plan-query (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'"))))

  )
