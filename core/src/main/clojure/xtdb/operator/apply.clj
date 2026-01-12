(ns xtdb.operator.apply
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr])
  (:import (xtdb ICursor)
           (xtdb.arrow RelationReader)
           (xtdb.operator.apply ApplyCursor ApplyMode$AntiJoin ApplyMode$CrossJoin ApplyMode$LeftJoin ApplyMode$MarkJoin ApplyMode$SemiJoin ApplyMode$SingleJoin DependentCursorFactory)))

(s/def ::mode #{:cross-join, :left-outer-join, :semi-join, :anti-join, :single-join, :mark-join})
(s/def ::columns (s/map-of ::lp/column ::lp/column, :conform-keys true))
(s/def ::mark-join-projection ::lp/column-expression)

(defmethod lp/ra-expr :apply [_]
  (s/cat :op #{:apply}
         :opts (s/keys :req-un [::mode ::columns]
                       :opt-un [::mark-join-projection])
         :independent-relation ::lp/ra-expression
         :dependent-relation ::lp/ra-expression))

(defn ->mode-strategy [mode mark-join-projection dependent-vec-types]
  (case mode
    :mark-join
    (let [[col-name _expr] (first mark-join-projection)]
      (ApplyMode$MarkJoin. (str col-name)))

    :cross-join (ApplyMode$CrossJoin. dependent-vec-types)
    :left-outer-join (ApplyMode$LeftJoin. dependent-vec-types)
    :semi-join ApplyMode$SemiJoin/INSTANCE
    :anti-join ApplyMode$AntiJoin/INSTANCE
    :single-join (ApplyMode$SingleJoin. dependent-vec-types)))

(defmethod lp/emit-expr :apply [{:keys [opts independent-relation dependent-relation]} args]
  (let [{:keys [mode columns mark-join-projection]} opts]
    ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?

    (lp/unary-expr (lp/emit-expr independent-relation args)
      (fn [{indep-vec-types :vec-types, :as indep-rel}]
        (let [{:keys [param-types] :as dependent-args} (-> args
                                                           (update :param-types
                                                                   (fnil into {})
                                                                   (map (fn [[ik dk]]
                                                                          (if-let [vec-type (get indep-vec-types ik)]
                                                                            [dk vec-type]
                                                                            (throw (err/incorrect :xtdb.apply/missing-column
                                                                                                  (str "Column missing from independent relation: " ik)
                                                                                                  {:column ik})))))
                                                                   columns))
              {dep-vec-types :vec-types, ->dependent-cursor :->cursor, :as dep-rel} (lp/emit-expr dependent-relation dependent-args)
              out-dep-vec-types (case mode
                                  :mark-join
                                  (let [[col-name _expr] (first mark-join-projection)]
                                    {col-name #xt/type [:? :bool]})

                                  :cross-join dep-vec-types

                                  (:left-outer-join :single-join) (types/with-nullable-types dep-vec-types)

                                  (:semi-join :anti-join) {})
              out-vec-types (merge-with types/merge-types indep-vec-types out-dep-vec-types)]
          {:op (case mode
                 :mark-join :apply-mark-join
                 :cross-join :apply-cross-join
                 :left-outer-join :apply-left-join
                 :semi-join :apply-semi-join
                 :anti-join :apply-anti-join
                 :single-join :apply-single-join)
           :children [indep-rel dep-rel]
           :explain {:columns (pr-str columns)}
           :vec-types out-vec-types

           :->cursor (let [out-dep-vec-types (update-keys out-dep-vec-types str)
                           mode-strat (->mode-strategy mode mark-join-projection out-dep-vec-types)
                           open-dependent-cursor
                           (if (= mode :mark-join)
                             (let [[_col-name form] (first mark-join-projection)
                                   input-types {:var-types dep-vec-types
                                                :param-types param-types}
                                   projection-spec (expr/->expression-projection-spec "_expr" (expr/form->expr form input-types) input-types)]
                               (fn [{:keys [allocator args explain-analyze? tracer query-span] :as query-opts}]
                                 (let [^ICursor dep-cursor (->dependent-cursor query-opts)]
                                   (cond-> (reify ICursor
                                             (getCursorType [_] "apply-mark-join")
                                             (getChildCursors [_] [dep-cursor])

                                             (tryAdvance [_ c]
                                               (.tryAdvance dep-cursor (fn [in-rel]
                                                                         (with-open [match-vec (.project projection-spec allocator in-rel {} args)]
                                                                           (.accept c (vr/rel-reader [match-vec]))))))

                                             (close [_] (.close dep-cursor)))
                                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))))
                             ->dependent-cursor)]

                       (fn [{:keys [allocator explain-analyze? tracer query-span] :as query-opts} independent-cursor]
                         (cond-> (ApplyCursor. allocator mode-strat independent-cursor out-dep-vec-types
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
                                                                                                             1))))))))
                           (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))})))))
