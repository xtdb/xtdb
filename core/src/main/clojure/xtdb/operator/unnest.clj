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
           (org.apache.arrow.vector IntVector)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Union Field FieldType)
           (xtdb ICursor)
           (xtdb.arrow RelationReader RowCopier VectorReader VectorWriter)
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
  (tryAdvance [_this c]
    (let [advanced? (boolean-array 1)]
      (while (and (.tryAdvance in-cursor
                               (fn [^RelationReader in-rel]
                                 (with-open [out-vec (.createVector to-field allocator)]
                                   (let [out-writer (vw/->writer out-vec)
                                         out-cols (LinkedList.)

                                         vec-rdr (.vectorForOrNull in-rel from-column-name)
                                         vec-type (.getType (.getField vec-rdr))

                                         rdrs+copiers
                                         (condp instance? vec-type
                                           ArrowType$List
                                           [[vec-rdr (-> (.getListElements vec-rdr)
                                                         (.rowCopier out-writer))]]

                                           SetType
                                           [[vec-rdr (-> (.getListElements vec-rdr)
                                                         (.rowCopier out-writer))]]

                                           ArrowType$Union
                                           (concat (when-let [list-rdr (.vectorForOrNull vec-rdr "list")]
                                                     [[list-rdr (-> (.rowCopier (.getListElements list-rdr) out-writer))]])
                                                   (when-let [set-rdr (.vectorForOrNull vec-rdr "set")]
                                                     [[set-rdr (-> (.rowCopier (.getListElements set-rdr) out-writer))]]))

                                           nil)

                                         idxs (IntStream/builder)

                                         ordinal-vec (when ordinality-column
                                                       (IntVector. ordinality-column
                                                                   (FieldType/notNullable #xt.arrow/type :i32)
                                                                   allocator))

                                         _ (when ordinal-vec
                                             (.add out-cols (vr/vec->reader ordinal-vec)))

                                         ^VectorWriter ordinal-wtr (some-> ordinal-vec vw/->writer)]

                                     (try
                                       (dotimes [n (.getValueCount vec-rdr)]
                                         (doseq [[^VectorReader coll-rdr, ^RowCopier el-copier] rdrs+copiers]
                                           (when (and coll-rdr (not (.isNull coll-rdr n)))
                                             (let [len (.getListCount coll-rdr n)
                                                   start-pos (.getListStartIndex coll-rdr n)]
                                               (dotimes [el-idx len]
                                                 (.copyRow el-copier (+ start-pos el-idx))
                                                 (.add idxs n)
                                                 (some-> ordinal-wtr (.writeInt (inc el-idx))))))))

                                       (let [idxs (.toArray (.build idxs))]
                                         (when (pos? (alength idxs))
                                           (some-> ordinal-vec (.setValueCount (alength idxs)))

                                           (doseq [^VectorReader in-col in-rel]
                                             (when (= from-column-name (.getName in-col))
                                               (.add out-cols (vw/vec-wtr->rdr out-writer)))

                                             (.add out-cols (.select in-col idxs)))

                                           (.accept c (vr/rel-reader out-cols (alength idxs)))
                                           (aset advanced? 0 true)))
                                       (finally
                                         (util/try-close ordinal-vec)))))))

                  (not (aget advanced? 0))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

(defmethod lp/emit-expr :unnest [{:keys [columns relation], {:keys [ordinality-column]} :opts}, op-args]
  (let [[to-col from-col] (first columns)]
    (lp/unary-expr (lp/emit-expr relation op-args)
                   (fn [fields]
                     (let [unnest-field (->> (get fields from-col)
                                             types/flatten-union-field
                                             (keep types/unnest-field)
                                             (apply types/merge-fields))]
                       {:fields (-> fields
                                    (assoc to-col (types/field-with-name unnest-field (str to-col)))
                                    (cond-> ordinality-column (assoc ordinality-column (types/col-type->field ordinality-column :i32))))
                        :->cursor (fn [{:keys [allocator]} in-cursor]
                                    (UnnestCursor. allocator in-cursor
                                                   (str from-col) (types/field-with-name unnest-field (str to-col))
                                                   (some-> ordinality-column str)))})))))
