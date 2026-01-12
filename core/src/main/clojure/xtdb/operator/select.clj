(ns xtdb.operator.select
  (:require [clojure.spec.alpha :as s]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types])
  (:import (xtdb ICursor)
           (xtdb.operator SelectCursor)))

(defmethod lp/ra-expr :select [_]
  (s/cat :op #{:σ :sigma :select}
         :opts (s/keys :req-un [::predicate])
         :relation ::lp/ra-expression))

(s/def ::predicate ::lp/expression)

(set! *unchecked-math* :warn-on-boxed)

(defmethod lp/emit-expr :select [{:keys [opts relation]} {:keys [param-types] :as args}]
  (let [{:keys [predicate]} opts]
    (lp/unary-expr (lp/emit-expr relation args)
      (fn [{inner-vec-types :vec-types, inner-stats :stats :as inner-rel}]
        (let [input-types {:var-types inner-vec-types
                           :param-types param-types}
              selector (expr/->expression-selection-spec (expr/form->expr predicate input-types) input-types)]
          {:op :select
           :stats inner-stats
           :children [inner-rel]
           :explain  {:predicate (pr-str predicate)}
           :vec-types inner-vec-types
           :fields (into {} (map (fn [[k v]] [k (types/->field v k)])) inner-vec-types)
           :->cursor (fn [{:keys [allocator args schema explain-analyze? tracer query-span]} in-cursor]
                       (cond-> (-> (SelectCursor. allocator in-cursor selector schema args)
                                   (coalesce/->coalescing-cursor allocator))
                         (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))))
