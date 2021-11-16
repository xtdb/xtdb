(ns core2.operator.order-by
  (:require [core2.expression.comparator :as expr.comp]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import clojure.lang.Keyword
           core2.ICursor
           core2.types.LegType
           core2.vector.IIndirectRelation
           [java.util Comparator List]
           [java.util.function Consumer ToIntFunction]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator))

(set! *unchecked-math* :warn-on-boxed)

(deftype OrderSpec [^String col-name, ^Keyword direction])

(defn ->order-spec [col-name direction]
  (OrderSpec. col-name direction))

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

(defn- sorted-idxs ^ints [^IIndirectRelation read-rel, ^List #_<OrderSpec> order-specs]
  (-> (IntStream/range 0 (.rowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc ^OrderSpec order-spec]
                         (let [^String col-name (.col-name order-spec)
                               read-col (.vectorForName read-rel col-name)
                               leg-types (iv/col->leg-types read-col)
                               ^LegType leg-type (if (= 1 (count leg-types))
                                                   (first leg-types)
                                                   (throw (UnsupportedOperationException.)))
                               read-col (iv/reader-for-type read-col leg-type)
                               read-vec (.getVector read-col)
                               col-comparator (expr.comp/->comparator (.arrowType leg-type))

                               ^Comparator
                               comparator (cond-> (reify Comparator
                                                    (compare [_ left right]
                                                      (.compareIdx col-comparator read-vec (.getIndex read-col left) read-vec (.getIndex read-col right))))
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
        (with-open [out-rel (iv/select read-rel (sorted-idxs read-rel order-specs))]
          (.accept c out-rel)
          true)
        false)))

  (close [_]
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs]
  (OrderByCursor. allocator in-cursor order-specs))
