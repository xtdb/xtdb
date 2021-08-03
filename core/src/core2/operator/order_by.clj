(ns core2.operator.order-by
  (:require [core2.expression.comparator :as expr.comp]
            [core2.relation :as rel]
            [core2.util :as util])
  (:import clojure.lang.Keyword
           core2.ICursor
           core2.relation.IReadRelation
           [java.util Comparator List]
           [java.util.function Consumer ToIntFunction]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.ArrowType))

(set! *unchecked-math* :warn-on-boxed)

(deftype OrderSpec [^String col-name, ^Keyword direction])

(defn ->order-spec [col-name direction]
  (OrderSpec. col-name direction))

(defn- accumulate-relations ^core2.relation.IReadRelation [allocator ^ICursor in-cursor]
  (let [append-rel (rel/->fresh-append-relation allocator)]
    (.forEachRemaining in-cursor
                       (reify Consumer
                         (accept [_ read-rel]
                           (rel/copy-rel-from append-rel read-rel))))
    (.read append-rel)))

(defn- sorted-idxs ^ints [^IReadRelation read-rel, ^List #_<OrderSpec> order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc ^OrderSpec order-spec]
                         (let [^String col-name (.col-name order-spec)
                               read-col (.readColumn read-rel col-name)
                               arrow-types (.arrowTypes read-col)
                               ^ArrowType arrow-type (if (= 1 (.size arrow-types))
                                                       (first arrow-types)
                                                       (throw (UnsupportedOperationException.)))
                               col-comparator (expr.comp/->comparator arrow-type)

                               ^Comparator
                               comparator (cond-> (reify Comparator
                                                    (compare [_ left right]
                                                      (.compareIdx col-comparator
                                                                   read-col left
                                                                   read-col right)))
                                            (= :desc (.direction order-spec)) (.reversed))]
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
                        ^List #_<OrderSpec> order-specs]
  ICursor
  (tryAdvance [_ c]
    (with-open [read-rel (accumulate-relations allocator in-cursor)]
      (if (pos? (.rowCount read-rel))
        (with-open [out-rel (rel/select read-rel (sorted-idxs read-rel order-specs))]
          (.accept c out-rel)
          true)
        false)))

  (close [_]
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs]
  (OrderByCursor. allocator in-cursor order-specs))
