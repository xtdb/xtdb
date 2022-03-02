(ns core2.qgm-test
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

                   [b3 :qgm.box/type :qgm.box.type/base-table]
                   [b3 :qgm.box.base-table/name quotations]

                   [p4 :qgm.predicate/expression (= q3__3_partno 1)]
                   [p4 :qgm.predicate/quantifiers q3__3]

                   [q3__3 :qgm.quantifier/columns [price partno]]
                   [q3__3 :qgm.quantifier/ranges-over b3]
                   [q3__3 :qgm.quantifier/type :qgm.quantifier.type/foreach]))

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT q3.price FROM quotations q3 WHERE q3.partno = 1"))))))

  (t/is (= (sort '([b2 :qgm.box/root? true]
                   [b2 :qgm.box/type :qgm.box.type/select]
                   [b2 :qgm.box.body/columns [q1__3_partno q1__3_descr q2__4_suppno]]
                   [b2 :qgm.box.body/distinct :qgm.box.body.distinct/enforce]
                   [b2 :qgm.box.body/quantifiers q1__3]
                   [b2 :qgm.box.body/quantifiers q2__4]
                   [b2 :qgm.box.head/columns [partno descr suppno]]
                   [b2 :qgm.box.head/distinct? true]

                   ;; base tables
                   [b3 :qgm.box/type :qgm.box.type/base-table]
                   [b3 :qgm.box.base-table/name inventory]

                   [b4 :qgm.box/type :qgm.box.type/base-table]
                   [b4 :qgm.box.base-table/name quotations]

                   ;; predicate
                   [p5 :qgm.predicate/expression (= q1__3_partno q2__4_partno)]
                   [p5 :qgm.predicate/quantifiers q1__3]
                   [p5 :qgm.predicate/quantifiers q2__4]

                   [p6 :qgm.predicate/expression (= q1__3_descr "engine")]
                   [p6 :qgm.predicate/quantifiers q1__3]

                   ;; quantifiers
                   [q1__3 :qgm.quantifier/columns [partno descr]]
                   [q1__3 :qgm.quantifier/ranges-over b3]
                   [q1__3 :qgm.quantifier/type :qgm.quantifier.type/foreach]

                   [q2__4 :qgm.quantifier/columns [suppno partno]]
                   [q2__4 :qgm.quantifier/ranges-over b4]
                   [q2__4 :qgm.quantifier/type :qgm.quantifier.type/foreach]))

           (sort (qgm/qgm
                  (z/vector-zip
                   (sql/parse "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
                         FROM inventory q1, quotations q2
                         WHERE q1.partno = q2.partno AND q1.descr= 'engine'")))))))
