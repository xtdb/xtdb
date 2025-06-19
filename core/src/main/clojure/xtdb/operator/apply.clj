(ns xtdb.operator.apply
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr])
  (:import (java.util.function Consumer)
           (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.operator.apply ApplyCursor ApplyMode$AntiJoin ApplyMode$CrossJoin ApplyMode$LeftJoin ApplyMode$MarkJoin ApplyMode$SemiJoin ApplyMode$SingleJoin DependentCursorFactory)))

(defmethod lp/ra-expr :apply [_]
  (s/cat :op #{:apply}
         :mode (s/or :mark-join (s/map-of #{:mark-join} ::lp/column-expression, :count 1, :conform-keys true)
                     :otherwise #{:cross-join, :left-outer-join, :semi-join, :anti-join, :single-join})
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true)
         :independent-relation ::lp/ra-expression
         :dependent-relation ::lp/ra-expression))

(defn ->mode-strategy [mode dependent-fields]
  (zmatch mode
    [:mark-join mark-spec]
    (let [[col-name _expr] (first (:mark-join mark-spec))]
      (ApplyMode$MarkJoin. (str col-name)))

    [:otherwise simple-mode]
    (case simple-mode
      :cross-join (ApplyMode$CrossJoin. dependent-fields)
      :left-outer-join (ApplyMode$LeftJoin. dependent-fields)
      :semi-join ApplyMode$SemiJoin/INSTANCE
      :anti-join ApplyMode$AntiJoin/INSTANCE
      :single-join (ApplyMode$SingleJoin. dependent-fields))))

(defmethod lp/emit-expr :apply [{:keys [mode columns independent-relation dependent-relation]} args]

  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?

  (lp/unary-expr (lp/emit-expr independent-relation args)
    (fn [independent-fields]
      (let [{:keys [param-fields] :as dependent-args} (-> args
                                                          (update :param-fields
                                                                  (fnil into {})
                                                                  (map (fn [[ik dk]]
                                                                         (if-let [field (get independent-fields ik)]
                                                                           [dk field]
                                                                           (throw
                                                                            (err/illegal-arg
                                                                             :xtdb.apply/missing-column
                                                                             {::err/message (str "Column missing from independent relation: " ik)
                                                                              :column ik})))))
                                                                  columns))
            {dependent-fields :fields, ->dependent-cursor :->cursor} (lp/emit-expr dependent-relation dependent-args)
            out-dependent-fields (zmatch mode
                                   [:mark-join mark-spec]
                                   (let [[col-name _expr] (first (:mark-join mark-spec))]
                                     {col-name (types/col-type->field col-name [:union #{:null :bool}])})

                                   [:otherwise simple-mode]
                                   (case simple-mode
                                     :cross-join dependent-fields

                                     (:left-outer-join :single-join) (types/with-nullable-fields dependent-fields)

                                     (:semi-join :anti-join) {}))]

        {:fields (merge-with types/merge-fields independent-fields out-dependent-fields)

         :->cursor (let [mode-strat (->mode-strategy mode (for [[col-name field] out-dependent-fields]
                                                            (types/field-with-name field (str col-name))))
                         open-dependent-cursor
                         (zmatch mode
                           [:mark-join mark-spec]
                           (let [[_col-name form] (first (:mark-join mark-spec))
                                 input-types {:col-types (update-vals dependent-fields types/field->col-type)
                                              :param-types (update-vals param-fields types/field->col-type)}
                                 projection-spec (expr/->expression-projection-spec "_expr" (expr/form->expr form input-types) input-types)]
                             (fn [{:keys [allocator args] :as query-opts}]
                               (let [^ICursor dep-cursor (->dependent-cursor query-opts)]
                                 (reify ICursor
                                   (tryAdvance [_ c]
                                     (.tryAdvance dep-cursor (fn [in-rel]
                                                               (with-open [match-vec (.project projection-spec allocator in-rel {} args)]
                                                                 (.accept c (vr/rel-reader [match-vec]))))))

                                   (close [_] (.close dep-cursor))))))

                           [:otherwise _] ->dependent-cursor)]

                     (fn [{:keys [allocator] :as query-opts} independent-cursor]
                       (ApplyCursor. allocator mode-strat independent-cursor
                                     (reify DependentCursorFactory
                                       (open [_this in-rel idx]
                                         (open-dependent-cursor (-> query-opts
                                                                    (update :args
                                                                            (fn [^RelationReader args]
                                                                              (RelationReader/from (concat args
                                                                                                           (for [[ik dk] columns]
                                                                                                             (-> (.vectorForOrNull in-rel (str ik))
                                                                                                                 (.select (int-array [idx]))
                                                                                                                 (.withName (str dk)))))
                                                                                                   1))))))))))}))))
