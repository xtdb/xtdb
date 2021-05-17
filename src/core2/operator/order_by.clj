(ns core2.operator.order-by
  (:require [core2.expression.comparator :as expr.comp]
            [core2.util :as util]
            [core2.vector :as vec])
  (:import clojure.lang.Keyword
           core2.ICursor
           [core2.vector IAppendRelation IReadColumn IReadRelation]
           [java.util Comparator EnumSet List]
           [java.util.function Consumer ToIntFunction]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.Types$MinorType))

(set! *unchecked-math* :warn-on-boxed)

(deftype OrderSpec [^String col-name, ^Keyword direction])

(defn ->order-spec [col-name direction]
  (OrderSpec. col-name direction))

(defn- accumulate-relations ^core2.vector.IReadRelation [allocator ^ICursor in-cursor]
  (let [append-rel (vec/->fresh-append-relation allocator)]
    (.forEachRemaining in-cursor
                       (reify Consumer
                         (accept [_ read-rel]
                           (vec/copy-rel-from append-rel read-rel))))
    (.read append-rel)))

(defn- sorted-idxs ^ints [^IReadRelation read-rel, ^List #_<OrderSpec> order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc ^OrderSpec order-spec]
                         (let [^String col-name (.col-name order-spec)
                               read-col (.readColumn read-rel col-name)
                               minor-types (.minorTypes read-col)
                               ^Types$MinorType minor-type (if (= 1 (.size minor-types))
                                                             (first minor-types)
                                                             (throw (UnsupportedOperationException.)))
                               col-comparator (expr.comp/->comparator minor-type)

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
    (let [read-rel (accumulate-relations allocator in-cursor)]
      (try
        (if (pos? (.rowCount read-rel))
          (let [out-rel (vec/select read-rel (sorted-idxs read-rel order-specs))]
            (try
              (.accept c out-rel)
              true
              (finally
                (util/try-close out-rel))))
          false)
        (finally
          (util/try-close read-rel)))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs]
  (OrderByCursor. allocator in-cursor order-specs))
