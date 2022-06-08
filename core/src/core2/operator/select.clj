(ns core2.operator.select
  (:require [clojure.spec.alpha :as s]
            [core2.coalesce :as coalesce]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.operator.IRelationSelector
           core2.vector.IIndirectRelation
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator))

(defmethod lp/ra-expr :select [_]
  (s/cat :op #{:σ :sigma :select}
         :predicate ::lp/expression
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype SelectCursor [^BufferAllocator allocator, ^ICursor in-cursor, ^IRelationSelector selector, params]
  ICursor
  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1)]
      (while (and (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel]
                                     (when-let [idxs (.select selector allocator in-rel params)]
                                       (when-not (zero? (alength idxs))
                                         (.accept c (iv/select in-rel idxs))
                                         (aset advanced? 0 true)))))))
                  (not (aget advanced? 0))))
      (aget advanced? 0)))

  (close [_]
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :select [{:keys [predicate relation]} {:keys [param-types] :as args}]
  (lp/unary-expr relation args
    (fn [inner-col-types]
      (let [selector (expr/->expression-relation-selector predicate {:col-types inner-col-types, :param-types param-types})]
        {:col-types inner-col-types
         :->cursor (fn [{:keys [allocator params]} in-cursor]
                     (-> (SelectCursor. allocator in-cursor selector params)
                         (coalesce/->coalescing-cursor allocator)))}))))
