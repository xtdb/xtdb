(ns xtdb.operator.unwind
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util LinkedList)
           (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector IntVector)
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList ArrowType$List Field]
           (xtdb ICursor)
           (xtdb.vector IRowCopier IVectorReader IVectorWriter RelationReader)
           xtdb.vector.extensions.SetType))

(s/def ::ordinality-column ::lp/column)

(defmethod lp/ra-expr :unwind [_]
  (s/cat :op #{:ω :unwind}
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true, :count 1)
         :opts (s/? (s/keys :opt-un [::ordinality-column]))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype UnwindCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^String from-column-name
                       ^Field to-field
                       ^String ordinality-column]
  ICursor
  (tryAdvance [_this c]
    (let [advanced? (boolean-array 1)]
      (while (and (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (with-open [out-vec (.createVector to-field allocator)]
                                     (let [out-writer (vw/->writer out-vec)
                                           out-cols (LinkedList.)

                                           ^RelationReader in-rel in-rel
                                           vec-rdr (.readerForName in-rel from-column-name)
                                           vec-type (.getType (.getField vec-rdr))

                                           rdrs+copiers
                                           (case vec-type
                                             (#xt.arrow/type :list, #xt.arrow/type :set)
                                             [[vec-rdr (-> (.listElementReader vec-rdr)
                                                           (.rowCopier out-writer))]]

                                             #xt.arrow/type :union
                                             (concat (when-let [list-rdr (.legReader vec-rdr :list)]
                                                       [[list-rdr (-> (.rowCopier (.listElementReader list-rdr) out-writer))]])
                                                     (when-let [set-rdr (.legReader vec-rdr :set)]
                                                       [[set-rdr (-> (.rowCopier (.listElementReader set-rdr) out-writer))]]))

                                             nil)

                                           idxs (IntStream/builder)

                                           ordinal-vec (when ordinality-column
                                                         (IntVector. ordinality-column allocator))

                                           _ (when ordinal-vec
                                               (.add out-cols (vr/vec->reader ordinal-vec)))

                                           ^IVectorWriter ordinal-wtr (some-> ordinal-vec vw/->writer)]

                                       (try
                                         (dotimes [n (.valueCount vec-rdr)]
                                           (doseq [[^IVectorReader coll-rdr, ^IRowCopier el-copier] rdrs+copiers]
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

                                             (doseq [^IVectorReader in-col in-rel]
                                               (when (= from-column-name (.getName in-col))
                                                 (.add out-cols (vw/vec-wtr->rdr out-writer)))

                                               (.add out-cols (.select in-col idxs)))

                                             (.accept c (vr/rel-reader out-cols (alength idxs)))
                                             (aset advanced? 0 true)))
                                         (finally
                                           (util/try-close ordinal-vec))))))))

                  (not (aget advanced? 0))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

;; cases
;; - list vec
;; - nullable list vec
;; - list DUV
;; - scalar non-list
;; - DUV non-list

;; make listElementReader return _something_ even if it's not a list-vec - nil?
;; make DUV reader pass all the calls through

(defmethod lp/emit-expr :unwind [{:keys [columns relation], {:keys [ordinality-column]} :opts}, op-args]
  (let [[to-col from-col] (first columns)]
    (lp/unary-expr (lp/emit-expr relation op-args)
      (fn [fields]
        (let [unwind-field (->> (get fields from-col)
                                types/flatten-union-field
                                (keep (fn [^Field field]
                                        (condp instance? (.getType field)
                                          ArrowType$List (first (.getChildren field))
                                          SetType (first (.getChildren field))
                                          ArrowType$FixedSizeList (first (.getChildren field))
                                          (types/col-type->field :null))))
                                (apply types/merge-fields))]

          {:fields (-> fields
                       (assoc to-col unwind-field)
                       (cond-> ordinality-column (assoc ordinality-column (types/col-type->field :i32))))
           :->cursor (fn [{:keys [allocator]} in-cursor]
                       (UnwindCursor. allocator in-cursor
                                      (str from-col) (types/field-with-name unwind-field (str to-col))
                                      (some-> ordinality-column name)))})))))
