(ns core2.sql.qgm-test
  (:require [clojure.test :as t]
            [core2.sql.qgm :as qgm]
            [core2.sql :as sql]
            [clojure.zip :as z]))

(t/deftest test-qgm
  (t/is (= (sort '[[b4 :qgm.box/type :qgm.box.type/select]
                   [b4 :qgm.box.head/distinct? false]
                   [b4 :qgm.box.head/columns [price]]
                   [b4 :qgm.box.body/columns [q3__28_price]]
                   [b4 :qgm.box.body/distinct :qgm.box.body.distinct/permit]
                   [b4 :qgm.box/root? true]
                   [bt_quotations :qgm.box/type :qgm.box.type/base-table]
                   [bt_quotations :qgm.box.base-table/name quotations]
                   [q3__28 :qgm.quantifier/ranges-over bt_quotations]
                   [q3__28 :qgm.quantifier/type :qgm.quantifier.type/foreach]
                   [q3__28 :qgm.quantifier/columns [price partno]]
                   [b4 :qgm.box.body/quantifiers q3__28]
                   [p41 :qgm.predicate/expression (= q3__28_partno 1)]
                   [p41 :qgm.predicate/quantifiers q3__28]])

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT q3.price FROM quotations q3 WHERE q3.partno = 1")))))

        "simple query")

  (t/is (= (sort '[[b4 :qgm.box/type :qgm.box.type/select]
                   [b4 :qgm.box.head/distinct? true]
                   [b4 :qgm.box.head/columns [partno descr suppno]]
                   [b4 :qgm.box.body/columns [q1__55_partno q1__55_descr q2__63_suppno]]
                   [b4 :qgm.box.body/distinct :qgm.box.body.distinct/enforce]
                   [b4 :qgm.box/root? true]
                   [bt_inventory :qgm.box/type :qgm.box.type/base-table]
                   [bt_inventory :qgm.box.base-table/name inventory]
                   [q1__55 :qgm.quantifier/ranges-over bt_inventory]
                   [q1__55 :qgm.quantifier/type :qgm.quantifier.type/foreach]
                   [q1__55 :qgm.quantifier/columns [partno descr]]
                   [b4 :qgm.box.body/quantifiers q1__55]
                   [bt_quotations :qgm.box/type :qgm.box.type/base-table]
                   [bt_quotations :qgm.box.base-table/name quotations]
                   [q2__63 :qgm.quantifier/ranges-over bt_quotations]
                   [q2__63 :qgm.quantifier/type :qgm.quantifier.type/foreach]
                   [q2__63 :qgm.quantifier/columns [suppno partno]]
                   [b4 :qgm.box.body/quantifiers q2__63]
                   [p78 :qgm.predicate/expression (= q1__55_partno q2__63_partno)]
                   [p78 :qgm.predicate/quantifiers q1__55]
                   [p78 :qgm.predicate/quantifiers q2__63]
                   [p108 :qgm.predicate/expression (= q1__55_descr "engine")]
                   [p108 :qgm.predicate/quantifiers q1__55]])

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
                               FROM inventory q1, quotations q2
                               WHERE q1.partno = q2.partno AND q1.descr= 'engine'")))))

        "add a join")

  (t/is (= '{q157
             {:db/id q157,
              :qgm.quantifier/type :qgm.quantifier.type/all,
              :qgm.quantifier/ranges-over b157,
              :qgm.quantifier/columns [price]},
             bt_inventory
             {:db/id bt_inventory,
              :qgm.box/type :qgm.box.type/base-table,
              :qgm.box.base-table/name inventory},
             b157
             {:db/id b157,
              :qgm.box/type :qgm.box.type/select,
              :qgm.box.head/distinct? false,
              :qgm.box.head/columns [price],
              :qgm.box.body/columns [q3__181_price],
              :qgm.box.body/distinct :qgm.box.body.distinct/permit,
              :qgm.box.body/quantifiers q3__181},
             p80
             {:db/id p80,
              :qgm.predicate/expression (= q1__55_partno q2__63_partno),
              :qgm.predicate/quantifiers #{q2__63 q1__55}},
             q2__63
             {:db/id q2__63,
              :qgm.quantifier/ranges-over bt_quotations,
              :qgm.quantifier/type :qgm.quantifier.type/foreach,
              :qgm.quantifier/columns [suppno partno price]},
             bt_quotations
             {:db/id bt_quotations,
              :qgm.box/type :qgm.box.type/base-table,
              :qgm.box.base-table/name quotations},
             hack-qp1
             {:db/id hack-qp1,
              :qgm.predicate/expression (<= q2__63_price q157__price),
              :qgm.predicate/quantifiers #{q157 q2__63}},
             p110
             {:db/id p110,
              :qgm.predicate/expression (= q1__55_descr "engine"),
              :qgm.predicate/quantifiers q1__55},
             p194
             {:db/id p194,
              :qgm.predicate/expression (= q2__63_partno q3__181_partno),
              :qgm.predicate/quantifiers #{q2__63 q3__181}},
             q1__55
             {:db/id q1__55,
              :qgm.quantifier/ranges-over bt_inventory,
              :qgm.quantifier/type :qgm.quantifier.type/foreach,
              :qgm.quantifier/columns [partno descr]},
             q3__181
             {:db/id q3__181,
              :qgm.quantifier/ranges-over bt_quotations,
              :qgm.quantifier/type :qgm.quantifier.type/foreach,
              :qgm.quantifier/columns [price partno]},
             b4
             {:db/id b4,
              :qgm.box/type :qgm.box.type/select,
              :qgm.box.head/distinct? true,
              :qgm.box.head/columns [partno descr suppno],
              :qgm.box.body/columns [q1__55_partno q1__55_descr q2__63_suppno],
              :qgm.box.body/distinct :qgm.box.body.distinct/enforce,
              :qgm.box/root? true,
              :qgm.box.body/quantifiers #{q157 q2__63 q1__55}}}

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

  (t/is (= '{q3__110
             {:db/id q3__110,
              :qgm.quantifier/ranges-over bt_inventory,
              :qgm.quantifier/type :qgm.quantifier.type/foreach,
              :qgm.quantifier/columns [partno onhand_qty type]},
             bt_inventory
             {:db/id bt_inventory,
              :qgm.box/type :qgm.box.type/base-table,
              :qgm.box.base-table/name inventory},
             q86
             {:db/id q86,
              :qgm.quantifier/type :qgm.quantifier.type/existential,
              :qgm.quantifier/ranges-over b86,
              :qgm.quantifier/columns [partno]},
             p155
             {:db/id p155,
              :qgm.predicate/expression (= q3__110_type "CPU"),
              :qgm.predicate/quantifiers q3__110},
             b86
             {:db/id b86,
              :qgm.box/type :qgm.box.type/select,
              :qgm.box.head/distinct? false,
              :qgm.box.head/columns [partno],
              :qgm.box.body/columns [q3__110_partno],
              :qgm.box.body/distinct :qgm.box.body.distinct/permit,
              :qgm.box.body/quantifiers q3__110},
             bt_quotations
             {:db/id bt_quotations,
              :qgm.box/type :qgm.box.type/base-table,
              :qgm.box.base-table/name quotations},
             hack-qp1
             {:db/id hack-qp1,
              :qgm.predicate/expression (= q1__52_partno q86__partno),
              :qgm.predicate/quantifiers #{q86 q1__52}},
             p125
             {:db/id p125,
              :qgm.predicate/expression (< q3__110_onhand_qty q1__52_order_qty),
              :qgm.predicate/quantifiers #{q3__110 q1__52}},
             q1__52
             {:db/id q1__52,
              :qgm.quantifier/ranges-over bt_quotations,
              :qgm.quantifier/type :qgm.quantifier.type/foreach,
              :qgm.quantifier/columns [partno price order_qty]},
             b4
             {:db/id b4,
              :qgm.box/type :qgm.box.type/select,
              :qgm.box.head/distinct? false,
              :qgm.box.head/columns [partno price order_qty],
              :qgm.box.body/columns [q1__52_partno q1__52_price q1__52_order_qty],
              :qgm.box.body/distinct :qgm.box.body.distinct/permit,
              :qgm.box/root? true,
              :qgm.box.body/quantifiers #{q86 q1__52}}}
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
  (t/is (= '{:plan [:rename {q3__28_price price}
                    [:project [q3__28_price]
                     [:select (= q3__28_partno 1)
                      [:rename q3__28 [:scan [price partno]]]]]]}

           (qgm/plan-query (sql/parse "
SELECT q3.price FROM quotations q3 WHERE q3.partno = 1"))))

  (t/is (= '{:plan [:distinct
                    [:rename {q1__55_partno partno, q1__55_descr descr, q2__63_suppno suppno}
                     [:project [q1__55_partno q1__55_descr q2__63_suppno]
                      [:select (and (= q1__55_descr "engine") (= q1__55_partno q2__63_partno))
                       [:cross-join
                        [:rename q2__63 [:scan [suppno partno]]]
                        [:rename q1__55 [:scan [partno descr]]]]]]]]}
           (qgm/plan-query (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'")))))
