(ns xtdb.operator.order-by
  (:require [clojure.spec.alpha :as s]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import (java.util Map)
           java.util.function.Supplier
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.arrow RelationReader)
           xtdb.api.ICursor
           (xtdb.operator ExternalSorter OrderSpec OrderSpec$Direction OrderSpec$NullOrdering OrderSpecKt)))

(s/def ::direction #{:asc :desc})
(s/def ::null-ordering #{:nulls-first :nulls-last})

(s/def ::order-specs (s/coll-of (-> (s/cat :column ::lp/column
                                           :spec-opts (s/? (s/keys :opt-un [::direction ::null-ordering])))
                                    (s/nonconforming))
                                :kind vector?))

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *block-size* (int 102400))

(defn- ->order-specs [order-specs]
  (mapv (fn [[column {:keys [direction null-ordering]}]]
          (OrderSpec. (str column)
                      (if (= :desc direction) OrderSpec$Direction/DESC OrderSpec$Direction/ASC)
                      (if (= :nulls-first null-ordering) OrderSpec$NullOrdering/NULLS_FIRST OrderSpec$NullOrdering/NULLS_LAST)))
        order-specs))

(defn sorted-idxs ^ints [^RelationReader read-rel, order-specs]
  (OrderSpecKt/sortedIdxs read-rel (->order-specs order-specs) expr/*default-tz*))

(defn ->external-sorter
  "Sorts pages with the columns in `vec-types` by `order-specs`, keeping only the first `row-limit` rows.
  Spills a sorted run to a temp dir whenever it holds more than `*block-size*` rows."
  ^ExternalSorter [^BufferAllocator allocator, vec-types, order-specs, ^long row-limit]
  (ExternalSorter. allocator ^Map (update-keys vec-types str)
                   (->order-specs order-specs) expr/*default-tz*
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
