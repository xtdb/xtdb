(ns xtdb.operator.select
  (:require [clojure.spec.alpha :as s]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util]
            [xtdb.vector.indirect :as iv])
  (:import xtdb.ICursor
           xtdb.operator.IRelationSelector
           xtdb.vector.IIndirectRelation
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
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [inner-col-types]
      (let [selector (expr/->expression-relation-selector predicate {:col-types inner-col-types, :param-types param-types})]
        {:col-types inner-col-types
         :->cursor (fn [{:keys [allocator params]} in-cursor]
                     (-> (SelectCursor. allocator in-cursor selector params)
                         (coalesce/->coalescing-cursor allocator)))}))))
