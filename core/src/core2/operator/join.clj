(ns core2.operator.join
  (:require [core2.bloom :as bloom]
            [core2.expression.hash :as hash]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [clojure.set :as set])
  (:import core2.ICursor
           core2.types.LegType
           [core2.vector IIndirectRelation IIndirectVector IRelationWriter IRowCopier IVectorWriter]
           [java.util ArrayList HashMap Iterator List Map]
           [java.util.function Consumer Function]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           org.roaringbitmap.buffer.MutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- cross-product ^core2.vector.IIndirectRelation [^IIndirectRelation left-rel, ^IIndirectRelation right-rel]
  (let [left-row-count (.rowCount left-rel)
        right-row-count (.rowCount right-rel)
        row-count (* left-row-count right-row-count)]
    (iv/->indirect-rel (concat (iv/select left-rel
                                          (let [idxs (int-array row-count)]
                                            (dotimes [idx row-count]
                                              (aset idxs idx ^long (quot idx right-row-count)))
                                            idxs))

                               (iv/select right-rel
                                          (let [idxs (int-array row-count)]
                                            (dotimes [idx row-count]
                                              (aset idxs idx ^long (rem idx right-row-count)))
                                            idxs))))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-rels
                          ^:unsynchronized-mutable ^Iterator left-rel-iterator
                          ^:unsynchronized-mutable ^IIndirectRelation right-rel]
  ICursor
  (tryAdvance [this c]
    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ left-rel]
                           (.add left-rels (iv/copy left-rel allocator)))))

    (boolean
     (when-let [right-rel (or (when (and left-rel-iterator (.hasNext left-rel-iterator))
                                right-rel)
                              (do
                                (when right-rel
                                  (.close right-rel)
                                  (set! (.right-rel this) nil))
                                (when (.tryAdvance right-cursor
                                                   (reify Consumer
                                                     (accept [_ right-rel]
                                                       (set! (.right-rel this) (iv/copy right-rel allocator))
                                                       (set! (.left-rel-iterator this) (.iterator left-rels)))))
                                  (.right-rel this))))]

       (when-let [left-rel (when (.hasNext left-rel-iterator)
                             (.next left-rel-iterator))]
         (.accept c (cross-product left-rel right-rel))
         true))))

  (close [_]
    (when left-rels
      (run! util/try-close left-rels)
      (.clear left-rels))
    (util/try-close left-cursor)
    (util/try-close right-rel)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.) nil nil))

(definterface IBuildRow
  (^void copyRow []))

(definterface IBuildPointer
  (^boolean isMatched [])
  (^void markMatched []))

(deftype BuildPointer [^List #_<IRowCopier> row-copiers, ^int idx, pointer-or-object,
                       ^:unsynchronized-mutable ^boolean matched?]
  IBuildPointer
  (isMatched [_] matched?)
  (markMatched [this] (set! (.matched? this) true))

  IBuildRow
  (copyRow [_]
    (doseq [^IRowCopier copier row-copiers]
      (.copyRow copier idx))))

(defn- ->null-build-row ^core2.operator.join.IBuildRow [^IRelationWriter rel-writer, ^List col-names]
  (let [cols (vec (for [col-name col-names]
                    (let [writer (.writerForName rel-writer col-name)
                          null-writer (-> (.asDenseUnion writer)
                                          (.writerForType LegType/NULL))]
                      (reify IBuildRow
                        (copyRow [_]
                          (.startValue writer)
                          (.startValue null-writer)
                          (.endValue null-writer)
                          (.endValue writer))))))]
    (reify IBuildRow
      (copyRow [_]
        (doseq [^IBuildRow col cols]
          (.copyRow col))))))

(defn- build-phase [^IRelationWriter rel-writer
                    ^IIndirectRelation build-rel
                    ^String build-column-name
                    ^String probe-column-name
                    ^Map join-key->build-pointers
                    ^MutableRoaringBitmap pushdown-bloom
                    semi-join?]
  (let [row-copiers (when-not semi-join?
                      (vec (for [^IIndirectVector col build-rel
                                 :let [col-name (.getName col)]
                                 :when (not= col-name probe-column-name)]
                             (vw/->row-copier (.writerForName rel-writer col-name) col))))
        build-col (.vectorForName build-rel build-column-name)
        hasher (hash/->hasher build-col)
        internal-vec (.getVector build-col)]
    (dotimes [build-idx (.getValueCount build-col)]
      (let [idx (.getIndex build-col build-idx)]
        (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec idx))
        (doto ^List (.computeIfAbsent join-key->build-pointers
                                      (.hashCode hasher build-idx)
                                      (reify Function
                                        (apply [_ _x]
                                          (ArrayList.))))
          (.add (BuildPointer. row-copiers build-idx
                               (util/pointer-or-object internal-vec idx)
                               false)))))))

(defn- probe-phase ^core2.vector.IIndirectRelation [^IRelationWriter rel-writer
                                                    ^IBuildRow null-build-row
                                                    ^IIndirectRelation probe-rel
                                                    ^Map join-key->build-pointers
                                                    ^String probe-column-name
                                                    join-type]
  (when (pos? (.rowCount probe-rel))
    (let [semi-join? (contains? #{:semi :anti-semi} join-type)
          anti-join? (= :anti-semi join-type)
          outer-join? (contains? #{:full-outer :left-outer} join-type)

          probe-row-copiers (vec (for [^IIndirectVector col probe-rel]
                                   (vw/->row-copier (.writerForName rel-writer (.getName col)) col)))
          probe-col (.vectorForName probe-rel probe-column-name)
          probe-pointer (ArrowBufPointer.)
          matching-build-rows (ArrayList.)
          matching-probe-idxs (IntStream/builder)
          internal-vec (.getVector probe-col)
          hasher (hash/->hasher probe-col)]

      (dotimes [probe-idx (.getValueCount probe-col)]
        (let [internal-idx (.getIndex probe-col probe-idx)
              match? (when-let [^List build-pointers (.get join-key->build-pointers (.hashCode hasher probe-idx))]
                       (let [probe-pointer-or-object (util/pointer-or-object internal-vec internal-idx probe-pointer)
                             build-pointer-it (.iterator build-pointers)]
                         (loop [match? false]
                           (if-let [^BuildPointer build-pointer (when (.hasNext build-pointer-it)
                                                                  (.next build-pointer-it))]
                             (cond
                               (not= (.pointer-or-object build-pointer) probe-pointer-or-object)
                               (recur match?)

                               semi-join? true

                               :else (do
                                       (.markMatched build-pointer)
                                       (.add matching-build-rows build-pointer)
                                       (.add matching-probe-idxs probe-idx)
                                       (recur true)))
                             match?))))]
          (cond
            outer-join? (when-not match?
                          (.add matching-probe-idxs probe-idx)
                          (.add matching-build-rows null-build-row))
            anti-join? (when-not match?
                         (.add matching-probe-idxs probe-idx))
            semi-join? (when match?
                         (.add matching-probe-idxs probe-idx)))))

      (when-not semi-join?
        (doseq [^IBuildRow build-row matching-build-rows]
          (.copyRow build-row)))

      (let [probe-idxs (.toArray (.build matching-probe-idxs))]
        (doseq [^IRowCopier row-copier probe-row-copiers]
          (dotimes [idx (alength probe-idxs)]
            (.copyRow row-copier (aget probe-idxs idx))))))))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, ^String build-column-name
                     ^ICursor probe-cursor, ^String probe-column-name
                     ^IRelationWriter rel-writer
                     ^List build-rels
                     ^Map join-key->build-pointers
                     ^MutableRoaringBitmap pushdown-bloom
                     join-type]
  ICursor
  (tryAdvance [_ c]
    (let [semi-join? (contains? #{:semi :anti-semi} join-type)]
      (.forEachRemaining build-cursor
                         (reify Consumer
                           (accept [_ build-rel]
                             (let [build-rel (iv/copy build-rel allocator)]
                               (.add build-rels build-rel)
                               (build-phase rel-writer build-rel
                                            build-column-name probe-column-name
                                            join-key->build-pointers pushdown-bloom
                                            semi-join?)))))

      (let [yield-missing? (contains? #{:anti-semi :left-outer :full-outer} join-type)
            build-rel-col-names (into #{}
                                      (mapcat (fn [^IIndirectRelation build-rel]
                                                (for [^IIndirectVector build-vec build-rel]
                                                  (.getName build-vec))))
                                      build-rels)]
        (boolean
         (or (when (or (not (.isEmpty join-key->build-pointers))
                       yield-missing?)
               (let [advanced? (boolean-array 1)]
                 (binding [scan/*column->pushdown-bloom* (if yield-missing?
                                                           scan/*column->pushdown-bloom*
                                                           (assoc scan/*column->pushdown-bloom* probe-column-name pushdown-bloom))]
                   (while (and (not (aget advanced? 0))
                               (.tryAdvance probe-cursor
                                            (reify Consumer
                                              (accept [_ probe-rel]
                                                (let [null-build-row (when (contains? #{:full-outer :left-outer} join-type)
                                                                       (->null-build-row rel-writer build-rel-col-names))]
                                                  (probe-phase rel-writer null-build-row
                                                               probe-rel join-key->build-pointers
                                                               probe-column-name join-type))

                                                (with-open [out-rel (vw/rel-writer->reader rel-writer)]
                                                  (try
                                                    (when (pos? (.rowCount out-rel))
                                                      (aset advanced? 0 true)
                                                      (.accept c out-rel))
                                                    (finally
                                                      (vw/clear-rel rel-writer))))))))))
                 (aget advanced? 0)))

             (when (and (= :full-outer join-type) (not (.isEmpty join-key->build-pointers)))
               (with-open [out-rel (vw/rel-writer->reader rel-writer)]
                 (let [null-build-row (->null-build-row rel-writer (set/difference (into #{}
                                                                                         (map (fn [^IVectorWriter w]
                                                                                                (.getName (.getVector w))))
                                                                                         rel-writer)
                                                                                   (set build-rel-col-names)))]
                   (doseq [build-pointers (vals join-key->build-pointers)
                           ^BuildPointer build-pointer build-pointers
                           :when (not (.isMatched build-pointer))]
                     (.copyRow build-pointer))
                   (.copyRow null-build-row))

                 (.clear join-key->build-pointers)
                 (when (pos? (.rowCount out-rel))
                   (.accept c out-rel)
                   true))))))))

  (close [_]
    (.clear pushdown-bloom)
    (run! util/try-close build-rels)
    (.clear build-rels)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor, ^String left-column-name,
                                         ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               left-cursor left-column-name right-cursor right-column-name
               (vw/->rel-writer allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               :inner))

(defn ->left-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                   ^ICursor left-cursor, ^String left-column-name,
                                                   ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               :semi))

(defn ->left-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, ^String left-column-name,
                                                    ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               :left-outer))

(defn ->full-outer-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                    ^ICursor left-cursor, ^String left-column-name,
                                                    ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               left-cursor left-column-name right-cursor right-column-name
               (vw/->rel-writer allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               :full-outer))

(defn ->left-anti-semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                                        ^ICursor left-cursor, ^String left-column-name,
                                                        ^ICursor right-cursor, ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (vw/->rel-writer allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               :anti-semi))
