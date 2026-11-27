(ns xtdb.operator.set
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.join :as join])
  (:import (xtdb.operator.set SetOperator$Difference SetOperator$Intersect SetOperator$Matching SetOperator$UnionAll)))

(defmethod lp/ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :opts map?
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defmethod lp/emit-expr :union-all [{:keys [_opts left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [op (SetOperator$UnionAll. (update-keys left-vec-types str) (update-keys right-vec-types str))]
                      {:op :union-all
                       :children [left-rel right-rel]
                       :vec-types (update-keys (.getVecTypes op) symbol)
                       :->cursor (fn [_ left-cursor right-cursor]
                                   (.openCursor op left-cursor right-cursor))}))))

(defn- emit-matching [op ->set-op {:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
                  (fn [{left-vec-types :vec-types :as left-rel} {right-vec-types :vec-types :as right-rel}]
                    (let [^SetOperator$Matching set-op (->set-op (update-keys left-vec-types str) (update-keys right-vec-types str))]
                      {:op op
                       :children [left-rel right-rel]
                       :vec-types (update-keys (.getVecTypes set-op) symbol)
                       :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                                   (.openCursor set-op allocator expr/*default-tz* join/*disk-join-threshold-rows*
                                                left-cursor right-cursor))}))))

(defmethod lp/emit-expr :intersect [expr args]
  (emit-matching :intersect #(SetOperator$Intersect. %1 %2) expr args))

(defmethod lp/emit-expr :difference [expr args]
  (emit-matching :difference #(SetOperator$Difference. %1 %2) expr args))
