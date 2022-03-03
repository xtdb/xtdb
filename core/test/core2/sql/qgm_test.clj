(ns core2.sql.qgm-test
  (:require [clojure.test :as t]
            [core2.sql.qgm :as qgm]
            [core2.sql :as sql]
            [clojure.zip :as z]))

(t/deftest test-qgm
  (t/is (= (sort '([b2 :qgm.box/root? true]
                   [b2 :qgm.box/type :qgm.box.type/select]
                   [b2 :qgm.box.body/columns [q3__3_price]]
                   [b2 :qgm.box.body/distinct :qgm.box.body.distinct/permit]
                   [b2 :qgm.box.body/quantifiers q3__3]
                   [b2 :qgm.box.head/columns [price]]
                   [b2 :qgm.box.head/distinct? false]

                   [bt_quotations :qgm.box/type :qgm.box.type/base-table]
                   [bt_quotations :qgm.box.base-table/name quotations]

                   [p4 :qgm.predicate/expression (= q3__3_partno 1)]
                   [p4 :qgm.predicate/quantifiers q3__3]

                   [q3__3 :qgm.quantifier/columns [price partno]]
                   [q3__3 :qgm.quantifier/ranges-over bt_quotations]
                   [q3__3 :qgm.quantifier/type :qgm.quantifier.type/foreach]))

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT q3.price FROM quotations q3 WHERE q3.partno = 1")))))

        "simple query")

  (t/is (= (sort '([b2 :qgm.box/root? true]
                   [b2 :qgm.box/type :qgm.box.type/select]
                   [b2 :qgm.box.body/columns [q1__3_partno q1__3_descr q2__4_suppno]]
                   [b2 :qgm.box.body/distinct :qgm.box.body.distinct/enforce]
                   [b2 :qgm.box.body/quantifiers q1__3]
                   [b2 :qgm.box.body/quantifiers q2__4]
                   [b2 :qgm.box.head/columns [partno descr suppno]]
                   [b2 :qgm.box.head/distinct? true]

                   ;; base tables
                   [bt_inventory :qgm.box/type :qgm.box.type/base-table]
                   [bt_inventory :qgm.box.base-table/name inventory]

                   [bt_quotations :qgm.box/type :qgm.box.type/base-table]
                   [bt_quotations :qgm.box.base-table/name quotations]

                   ;; predicate
                   [p5 :qgm.predicate/expression (= q1__3_partno q2__4_partno)]
                   [p5 :qgm.predicate/quantifiers q1__3]
                   [p5 :qgm.predicate/quantifiers q2__4]

                   [p6 :qgm.predicate/expression (= q1__3_descr "engine")]
                   [p6 :qgm.predicate/quantifiers q1__3]

                   ;; quantifiers
                   [q1__3 :qgm.quantifier/columns [partno descr]]
                   [q1__3 :qgm.quantifier/ranges-over bt_inventory]
                   [q1__3 :qgm.quantifier/type :qgm.quantifier.type/foreach]

                   [q2__4 :qgm.quantifier/columns [suppno partno]]
                   [q2__4 :qgm.quantifier/ranges-over bt_quotations]
                   [q2__4 :qgm.quantifier/type :qgm.quantifier.type/foreach]))

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
                               FROM inventory q1, quotations q2
                               WHERE q1.partno = q2.partno AND q1.descr= 'engine'")))))

        "add a join")

  (t/is (= '{b2 {:db/id b2,
                 :qgm.box/type :qgm.box.type/select,
                 :qgm.box.head/distinct? true,
                 :qgm.box.head/columns [partno descr suppno],
                 :qgm.box.body/columns [q1__3_partno q1__3_descr q2__4_suppno],
                 :qgm.box.body/distinct :qgm.box.body.distinct/enforce,
                 :qgm.box/root? true,
                 :qgm.box.body/quantifiers #{q8 q2__4 q1__3}},
             b8 {:db/id b8,
                 :qgm.box/type :qgm.box.type/select,
                 :qgm.box.head/distinct? false,
                 :qgm.box.head/columns [price],
                 :qgm.box.body/columns [q3__9_price],
                 :qgm.box.body/distinct :qgm.box.body.distinct/permit,
                 :qgm.box.body/quantifiers q3__9},

             bt_inventory {:db/id bt_inventory,
                           :qgm.box/type :qgm.box.type/base-table,
                           :qgm.box.base-table/name inventory},

             bt_quotations {:db/id bt_quotations,
                            :qgm.box/type :qgm.box.type/base-table,
                            :qgm.box.base-table/name quotations},

             q1__3 {:db/id q1__3,
                    :qgm.quantifier/ranges-over bt_inventory,
                    :qgm.quantifier/type :qgm.quantifier.type/foreach,
                    :qgm.quantifier/columns [partno descr]},
             q2__4 {:db/id q2__4,
                    :qgm.quantifier/ranges-over bt_quotations,
                    :qgm.quantifier/type :qgm.quantifier.type/foreach,
                    :qgm.quantifier/columns [suppno partno price]},
             q3__9 {:db/id q3__9,
                    :qgm.quantifier/ranges-over bt_quotations,
                    :qgm.quantifier/type :qgm.quantifier.type/foreach,
                    :qgm.quantifier/columns [price partno]},

             q8 {:db/id q8
                 :qgm.quantifier/type :qgm.quantifier.type/all
                 :qgm.quantifier/columns [price]
                 :qgm.quantifier/ranges-over b8}

             p5 {:db/id p5,
                 :qgm.predicate/expression (= q1__3_partno q2__4_partno),
                 :qgm.predicate/quantifiers #{q2__4 q1__3}},

             p6 {:db/id p6,
                 :qgm.predicate/expression (= q1__3_descr "engine"),
                 :qgm.predicate/quantifiers q1__3}

             p10 {:db/id p10,
                  :qgm.predicate/expression (= q2__4_partno q3__9_partno),
                  :qgm.predicate/quantifiers #{q3__9 q2__4}}

             hack-qp1 {:db/id hack-qp1
                       :qgm.predicate/expression (<= q2__4_price q8__price)
                       :qgm.predicate/quantifiers #{q2__4 q8}}}

           (-> (sql/parse "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
                           FROM inventory q1, quotations q2
                           WHERE q1.partno = q2.partno AND q1.descr= 'engine'
                             AND q2.price <= ALL
                                      (SELECT q3.price FROM quotations q3
                                       WHERE q2.partno=q3.partno)")
               z/vector-zip
               qgm/qgm
               qgm/qgm->entities
               (->> (into {} (map (juxt :db/id identity))))))

        "all correlated sub-query")

  (t/is (= '{b2 {:db/id b2,
                 :qgm.box/type :qgm.box.type/select,
                 :qgm.box.head/distinct? false,
                 :qgm.box.head/columns [partno price order_qty],
                 :qgm.box.body/columns [q1__3_partno q1__3_price q1__3_order_qty],
                 :qgm.box.body/distinct :qgm.box.body.distinct/permit,
                 :qgm.box/root? true,
                 :qgm.box.body/quantifiers #{q1__3 q5}},
             b5 {:db/id b5,
                 :qgm.box/type :qgm.box.type/select,
                 :qgm.box.head/distinct? false,
                 :qgm.box.head/columns [partno],
                 :qgm.box.body/columns [q3__6_partno],
                 :qgm.box.body/distinct :qgm.box.body.distinct/permit,
                 :qgm.box.body/quantifiers q3__6},
             bt_inventory {:db/id bt_inventory,
                           :qgm.box/type :qgm.box.type/base-table,
                           :qgm.box.base-table/name inventory},
             bt_quotations {:db/id bt_quotations,
                            :qgm.box/type :qgm.box.type/base-table,
                            :qgm.box.base-table/name quotations},
             q1__3 {:db/id q1__3,
                    :qgm.quantifier/ranges-over bt_quotations,
                    :qgm.quantifier/type :qgm.quantifier.type/foreach,
                    :qgm.quantifier/columns [partno price order_qty]},
             q3__6 {:db/id q3__6,
                    :qgm.quantifier/ranges-over bt_inventory,
                    :qgm.quantifier/type :qgm.quantifier.type/foreach,
                    :qgm.quantifier/columns [partno onhand_qty type]},
             q5 {:db/id q5,
                 :qgm.quantifier/type :qgm.quantifier.type/existential,
                 :qgm.quantifier/ranges-over b5,
                 :qgm.quantifier/columns [partno]}
             p7 {:db/id p7,
                 :qgm.predicate/expression (< q3__6_onhand_qty q1__3_order_qty),
                 :qgm.predicate/quantifiers #{q3__6 q1__3}},
             p8 {:db/id p8,
                 :qgm.predicate/expression (= q3__6_type "CPU"),
                 :qgm.predicate/quantifiers q3__6}
             hack-qp1
             {:db/id hack-qp1,
              :qgm.predicate/expression (= q1__3_partno q5__partno),
              :qgm.predicate/quantifiers #{q1__3 q5}}}
           (-> (sql/parse "SELECT q1.partno, q1.price, q1.order_qty
             FROM quotations q1
             WHERE q1.partno IN
               (SELECT q3.partno
                FROM inventory q3
                WHERE q3.onhand_qty < q1.order_qty AND q3.type = 'CPU')")
               z/vector-zip
               qgm/qgm
               qgm/qgm->entities
               (->> (into {} (map (juxt :db/id identity))))))
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
                        [:rename q2__4 [:scan [suppno partno]]]
                        [:rename q1__3 [:scan [partno descr]]]]]]]]}
           (qgm/plan-query (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'"))))

  )
