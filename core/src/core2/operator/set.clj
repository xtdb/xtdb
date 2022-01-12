(ns core2.operator.set
  (:require [core2.expression.map :as emap]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.expression.map.IRelationMap
           core2.ICursor
           [core2.vector IIndirectRelation IIndirectVector]
           [java.util ArrayList HashSet LinkedList List Set]
           java.util.function.Consumer
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer))

(set! *unchecked-math* :warn-on-boxed)

(defn union-compatible-col-names [^ICursor left, ^ICursor right]
  (let [left-col-names (.getColumnNames left)
        right-col-names (.getColumnNames right)]
    (when-not (= left-col-names right-col-names)
      (throw (IllegalArgumentException. (format "union incompatible cols: %s vs %s" (pr-str left-col-names) (pr-str right-col-names)))))

    left-col-names))

(deftype UnionAllCursor [^Set col-names
                         ^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (getColumnNames [_] col-names)

  (tryAdvance [_ c]
    (boolean
     (or (.tryAdvance left-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IIndirectRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel))))))

         (.tryAdvance right-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IIndirectRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel)))))))))

  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->union-all-cursor ^core2.ICursor [^ICursor left-cursor, ^ICursor right-cursor]
  (UnionAllCursor. (union-compatible-col-names left-cursor right-cursor)
                   left-cursor right-cursor))

(deftype IntersectionCursor [^Set col-names
                             ^ICursor left-cursor
                             ^ICursor right-cursor
                             ^IRelationMap rel-map
                             difference?]
  ICursor
  (getColumnNames [_] col-names)
  (tryAdvance [_ c]
    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (let [^IIndirectRelation in-rel in-rel
                                 builder (.buildFromRelation rel-map in-rel)]
                             (dotimes [idx (.rowCount in-rel)]
                               (.addIfNotPresent builder idx))))))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance left-cursor
                                (reify Consumer
                                  (accept [_ in-rel]
                                    (let [^IIndirectRelation in-rel in-rel
                                          row-count (.rowCount in-rel)
                                          prober (.probeFromRelation rel-map in-rel)]

                                      (when (pos? row-count)
                                        (let [idxs (IntStream/builder)]
                                          (dotimes [idx row-count]
                                            (when (cond-> (pos? (.indexOf prober idx))
                                                    difference? not)
                                              (.add idxs idx)))

                                          (let [idxs (.toArray (.build idxs))]
                                            (when-not (empty? idxs)
                                              (aset advanced? 0 true)
                                              (.accept c (iv/select in-rel idxs))))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close rel-map)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->difference-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (let [col-names (union-compatible-col-names left-cursor right-cursor)]
    (IntersectionCursor. col-names
                         left-cursor right-cursor
                         (emap/->relation-map allocator {:key-col-names col-names})
                         true)))

(defn ->intersection-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (let [col-names (union-compatible-col-names left-cursor right-cursor)]
    (IntersectionCursor. col-names
                         left-cursor right-cursor
                         (emap/->relation-map allocator {:key-col-names col-names})
                         false)))

(deftype DistinctCursor [^ICursor in-cursor
                         ^IRelationMap rel-map]
  ICursor
  (getColumnNames [_] (.getColumnNames in-cursor))

  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         row-count (.rowCount in-rel)]
                                     (when (pos? row-count)
                                       (let [builder (.buildFromRelation rel-map in-rel)
                                             idxs (IntStream/builder)]
                                         (dotimes [idx row-count]
                                           (when (neg? (.addIfNotPresent builder idx))
                                             (.add idxs idx)))

                                         (let [idxs (.toArray (.build idxs))]
                                           (when-not (empty? idxs)
                                             (aset advanced? 0 true)
                                             (.accept c (iv/select in-rel idxs))))))))))))
      (aget advanced? 0)))

  (close [_]
    (util/try-close rel-map)
    (util/try-close in-cursor)))

(defn ->distinct-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor]
  (DistinctCursor. in-cursor (emap/->relation-map allocator {:key-col-names (vec (.getColumnNames in-cursor))})))

(defn- ->set-key [^List cols ^long idx]
  (let [set-key (ArrayList. (count cols))]
    (doseq [^IIndirectVector col cols]
      (.add set-key (util/pointer-or-object (.getVector col) (.getIndex col idx))))
    set-key))

(defn- copy-set-key [^BufferAllocator allocator ^List k]
  (dotimes [n (.size k)]
    (let [x (.get k n)]
      (.set k n (util/maybe-copy-pointer allocator x))))
  k)

(defn- release-set-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (util/try-close (.getBuf ^ArrowBufPointer x)))
  k)

(definterface ICursorFactory
  (^core2.ICursor createCursor []))

(definterface IFixpointCursorFactory
  (^core2.ICursor createCursor [^core2.operator.set.ICursorFactory cursor-factory]))

;; https://core.ac.uk/download/pdf/11454271.pdf "Algebraic optimization of recursive queries"
;; http://webdam.inria.fr/Alice/pdfs/Chapter-14.pdf "Recursion and Negation"

(defn ->fixpoint-cursor-factory [rels incremental?]
  (reify ICursorFactory
    (createCursor [_]
      (let [rels-queue (LinkedList. rels)]
        (reify
          ICursor
          (tryAdvance [_ c]
            (if-let [rel (.poll rels-queue)]
              (do
                (.accept c rel)
                true)
              false))

          (close [_]
            (when incremental?
              (run! util/try-close rels))))))))

(deftype FixpointCursor [^BufferAllocator allocator
                         ^ICursor base-cursor
                         ^IFixpointCursorFactory recursive-cursor-factory
                         ^Set fixpoint-set
                         ^List rels
                         incremental?
                         ^:unsynchronized-mutable ^ICursor recursive-cursor
                         ^:unsynchronized-mutable continue?]
  ICursor
  ;; HACK assumes recursive-cursor-factory has the same cols
  (getColumnNames [_] (.getColumnNames base-cursor))

  (tryAdvance [this c]
    (if-not (or continue? recursive-cursor)
      false

      (let [advanced? (boolean-array 1)
            inner-c (reify Consumer
                      (accept [_ in-rel]
                        (let [^IIndirectRelation in-rel in-rel]
                          (when (pos? (.rowCount in-rel))
                            (let [cols (seq in-rel)
                                  idxs (IntStream/builder)]
                              (dotimes [idx (.rowCount in-rel)]
                                (let [k (->set-key cols idx)]
                                  (when-not (.contains fixpoint-set k)
                                    (.add fixpoint-set (copy-set-key allocator k))
                                    (.add idxs idx))))

                              (let [idxs (.toArray (.build idxs))]
                                (when-not (empty? idxs)
                                  (let [out-rel (-> (iv/select in-rel idxs)
                                                    (iv/copy allocator))]
                                    (.add rels out-rel)
                                    (.accept c out-rel)
                                    (set! (.continue? this) true)
                                    (aset advanced? 0 true)))))))))]

        (.tryAdvance base-cursor inner-c)

        (or (aget advanced? 0)
            (do
              (while (and (not (aget advanced? 0)) continue?)
                (when-let [recursive-cursor (or recursive-cursor
                                                (when continue?
                                                  (set! (.continue? this) false)
                                                  (let [cursor (.createCursor recursive-cursor-factory
                                                                              (->fixpoint-cursor-factory (vec rels) incremental?))]
                                                    (when incremental? (.clear rels))
                                                    (set! (.recursive-cursor this) cursor)
                                                    cursor)))]


                  (while (and (not (aget advanced? 0))
                              (let [more? (.tryAdvance recursive-cursor inner-c)]
                                (when-not more?
                                  (util/try-close recursive-cursor)
                                  (set! (.recursive-cursor this) nil))
                                more?)))))
              (aget advanced? 0))))))

  (close [_]
    (util/try-close recursive-cursor)
    (run! util/try-close rels)
    (run! release-set-key fixpoint-set)
    (.clear fixpoint-set)
    (util/try-close base-cursor)))

(defn ->fixpoint-cursor ^core2.ICursor [^BufferAllocator allocator,
                                        ^ICursor base-cursor
                                        ^IFixpointCursorFactory recursive-cursor-factory
                                        incremental?]
  (FixpointCursor. allocator base-cursor recursive-cursor-factory
                   (HashSet.) (LinkedList.) incremental? nil true))
