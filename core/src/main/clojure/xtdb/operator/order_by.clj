(ns xtdb.operator.order-by
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression.comparator :as expr.comp]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import (java.util Comparator Map)
           java.util.function.Supplier
           java.util.stream.IntStream
           (org.apache.arrow.memory BufferAllocator)
           xtdb.arrow.RelationReader
           xtdb.api.ICursor
           (xtdb.operator ExternalSorter ExternalSorter$ComparatorFactory)))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(s/def ::order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                           :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                    (s/nonconforming))
                                :kind vector?))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *block-size* (int 102400))

;; :desc flips null-ordering before negating, not after - negating the
;; null outcomes too would move NULLS FIRST/LAST with direction, when
;; it's meant to be a fixed position (#6172).
(defn- ->order-comparator ^Comparator [read-col1 read-col2 direction null-ordering]
  (let [desc? (= :desc direction)
        col-comparator (expr.comp/->comparator read-col1 read-col2
                                                (if desc?
                                                  (case null-ordering
                                                    :nulls-first :nulls-last
                                                    :nulls-last :nulls-first)
                                                  null-ordering))]
    (if desc?
      (fn [left right] (- (.applyAsInt col-comparator left right)))
      (fn [left right] (.applyAsInt col-comparator left right)))))

(defn sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (-> (IntStream/range 0 (.getRowCount read-rel))
      (.boxed)
      (.sorted (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                                      :or {direction :asc, null-ordering :nulls-last}}]]
                         (let [read-col (.vectorForOrNull read-rel (str column))
                               comparator (->order-comparator read-col read-col direction null-ordering)]
                           (if acc
                             (.thenComparing acc comparator)
                             comparator)))
                       nil
                       order-specs))
      (.mapToInt identity)
      (.toArray)))

(defn mk-rel-comparator ^Comparator [^RelationReader rel1, ^RelationReader rel2, order-specs]
  (reduce (fn [^Comparator acc, [column {:keys [direction null-ordering]
                                         :or {direction :asc, null-ordering :nulls-last}}]]
            (let [read-col1 (.vectorFor rel1 (str column))
                  read-col2 (.vectorFor rel2 (str column))
                  comparator (->order-comparator read-col1 read-col2 direction null-ordering)]
              (if acc
                (.thenComparing acc comparator)
                comparator)))
          nil
          order-specs))

(defn ->external-sorter
  "Sorts pages with the columns in `vec-types` by `order-specs`, keeping only the first `row-limit` rows.
  Spills a sorted run to a temp dir whenever it holds more than `*block-size*` rows."
  ^ExternalSorter [^BufferAllocator allocator, vec-types, order-specs, ^long row-limit]
  (ExternalSorter. allocator ^Map (update-keys vec-types str)
                   (reify ExternalSorter$ComparatorFactory
                     (build [_ left right] (mk-rel-comparator left right order-specs)))
                   row-limit (int *block-size*)
                   (reify Supplier
                     (get [_] (util/tmp-dir "external-sort")))))

(deftype OrderByCursor [^ICursor in-cursor
                        ^ExternalSorter sorter
                        ^:unsynchronized-mutable ^ICursor sorted]
  ICursor
  (getCursorType [_] "order-by")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [_ c]
    (when-not sorted
      (.forEachRemaining in-cursor (fn [^RelationReader rel] (.append sorter rel)))
      (set! sorted (.finish sorter)))
    (.tryAdvance sorted c))

  (close [_]
    (util/try-close sorter)
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^ICursor [allocator in-cursor vec-types order-specs row-limit]
  (OrderByCursor. in-cursor (->external-sorter allocator vec-types order-specs row-limit) nil))
