(ns xtdb.operator.unnest
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util LinkedList)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Union Field FieldType)
           (xtdb ICursor)
           (xtdb.arrow IntVector RelationReader RowCopier VectorReader VectorWriter Vector)
           xtdb.vector.extensions.SetType))

(s/def ::ordinality-column ::lp/column)

(defmethod lp/ra-expr :unnest [_]
  (s/cat :op #{:ω :unnest}
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true, :count 1)
         :opts (s/? (s/keys :opt-un [::ordinality-column]))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype UnnestCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^String from-column-name
                       ^Field to-field
                       ^String ordinality-column]
  ICursor
  (getCursorType [_] "unnest")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [_this c]
    (let [advanced? (boolean-array 1)]
      (while (and (.tryAdvance in-cursor
                               (fn [^RelationReader in-rel]
                                 (with-open [out-vec (Vector/open allocator to-field)]
                                   (let [out-cols (LinkedList.)

                                         vec-rdr (.vectorForOrNull in-rel from-column-name)
                                         vec-type (.getArrowType vec-rdr)

                                         rdrs+copiers
                                         (condp instance? vec-type
                                           ArrowType$List
                                           [[vec-rdr (-> (.getListElements vec-rdr)
                                                         (.rowCopier out-vec))]]

                                           SetType
                                           [[vec-rdr (-> (.getListElements vec-rdr)
                                                         (.rowCopier out-vec))]]

                                           ArrowType$Union
                                           (concat (when-let [list-rdr (.vectorForOrNull vec-rdr "list")]
                                                     [[list-rdr (-> (.rowCopier (.getListElements list-rdr) out-vec))]])
                                                   (when-let [set-rdr (.vectorForOrNull vec-rdr "set")]
                                                     [[set-rdr (-> (.rowCopier (.getListElements set-rdr) out-vec))]]))

                                           nil)

                                         idxs (IntStream/builder)]

                                     (util/with-open [ordinal-vec (when ordinality-column
                                                                    (IntVector/open allocator ordinality-column false 0))]
                                       (when ordinal-vec
                                         (.add out-cols ordinal-vec))

                                       (dotimes [n (.getValueCount vec-rdr)]
                                         (doseq [[^VectorReader coll-rdr, ^RowCopier el-copier] rdrs+copiers]
                                           (when (and coll-rdr (not (.isNull coll-rdr n)))
                                             (let [len (.getListCount coll-rdr n)
                                                   start-pos (.getListStartIndex coll-rdr n)]
                                               (.copyRange el-copier start-pos len)

                                               (dotimes [el-idx len]
                                                 (.add idxs n)
                                                 (some-> ordinal-vec (.writeInt (inc el-idx))))))))

                                       (let [idxs (.toArray (.build idxs))]
                                         (when (pos? (alength idxs))
                                           (doseq [^VectorReader in-col in-rel]
                                             (when (= from-column-name (.getName in-col))
                                               (.add out-cols out-vec))

                                             (.add out-cols (.select in-col idxs)))

                                           (.accept c (vr/rel-reader out-cols (alength idxs)))
                                           (aset advanced? 0 true))))))))

                  (not (aget advanced? 0))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

(defmethod lp/emit-expr :unnest [{:keys [columns relation], {:keys [ordinality-column]} :opts}, op-args]
  (let [[to-col from-col] (first columns)]
    (lp/unary-expr (lp/emit-expr relation op-args)
                   (fn [{:keys [fields] :as inner-rel}]
                     (let [unnest-field (->> (get fields from-col)
                                             types/flatten-union-field
                                             (keep types/unnest-field)
                                             (apply types/merge-fields))
                           out-fields (-> fields
                                          (assoc to-col (types/field-with-name unnest-field (str to-col)))
                                          (cond-> ordinality-column (assoc ordinality-column (types/->field :i32 ordinality-column))))]
                       {:op :unnest
                        :children [inner-rel]
                        :explain {:from from-col
                                  :to to-col
                                  :ordinality ordinality-column}
                        :fields out-fields
                        :vec-types (update-vals out-fields types/->type)
                        :->cursor (fn [{:keys [allocator explain-analyze? tracer query-span]} in-cursor]
                                    (cond-> (UnnestCursor. allocator in-cursor
                                                           (str from-col) (types/field-with-name unnest-field (str to-col))
                                                           (some-> ordinality-column str))
                                      (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span)))})))))
