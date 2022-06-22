(ns core2.operator.order-by
  (:require [clojure.spec.alpha :as s]
            [core2.expression.comparator :as expr.comp]
            [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import core2.ICursor
           core2.vector.IIndirectRelation
           [java.util Comparator]
           [java.util.function Consumer ToIntFunction]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(defmethod lp/ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                            :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                     (s/nonconforming))
                                 :kind vector?)
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- accumulate-relations ^core2.vector.IIndirectRelation [allocator ^ICursor in-cursor]
  (let [rel-writer (vw/->rel-writer allocator)]
    (try
      (.forEachRemaining in-cursor
                         (reify Consumer
                           (accept [_ src-rel]
                             (vw/append-rel rel-writer src-rel))))
      (catch Exception e
        (.close rel-writer)
        (throw e)))

    (vw/rel-writer->reader rel-writer)))

(defn- sorted-idxs ^ints [^IIndirectRelation read-rel, order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (.vectorForName read-rel (name column))
                               col-comparator (expr.comp/->comparator read-col read-col null-ordering)

                               ^Comparator
                               comparator (cond-> (reify Comparator
                                                    (compare [_ left right]
                                                      (.applyAsInt col-comparator left right)))
                                            (= :desc direction) (.reversed))]
                           (if acc
                             (.thenComparing acc comparator)
                             comparator)))
                       nil
                       order-specs))
      (.mapToInt (reify ToIntFunction
                   (applyAsInt [_ x] x)))
      (.toArray)))

(deftype OrderByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        order-specs]
  ICursor
  (tryAdvance [_ c]
    (with-open [read-rel (accumulate-relations allocator in-cursor)]
      (if (pos? (.rowCount read-rel))
        (with-open [out-rel (iv/select read-rel (sorted-idxs read-rel order-specs))]
          (.accept c out-rel)
          true)
        false)))

  (close [_]
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :order-by [{:keys [order-specs relation]} args]
  (lp/unary-expr relation args
    (fn [col-types]
      {:col-types col-types
       :->cursor (fn [{:keys [allocator]} in-cursor]
                   (OrderByCursor. allocator in-cursor order-specs))})))
