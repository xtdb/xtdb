(ns core2.operator.join
  (:require [core2.bloom :as bloom]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.relation :as rel])
  (:import core2.ICursor
           [core2.relation IReadColumn IReadRelation]
           [java.util ArrayList HashMap List Map]
           [java.util.function Consumer Function]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           org.roaringbitmap.buffer.MutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- cross-product ^core2.relation.IReadRelation [^List left-rels, ^long left-row-count, ^IReadRelation right-rel]
  (let [out-rel (rel/->indirect-append-relation)
        right-row-count (.rowCount right-rel)]

    (doseq [^IReadRelation left-rel left-rels
            :let [left-row-count (.rowCount left-rel)]
            ^IReadColumn in-col (.readColumns left-rel)
            :let [out-col (.appendColumn out-rel (.getName in-col))]]
      (dotimes [left-idx left-row-count]
        (dotimes [_right-idx right-row-count]
          (.appendFrom out-col in-col left-idx))))

    (doseq [^IReadColumn in-col (.readColumns right-rel)
            :let [out-col (.appendColumn out-rel (.getName in-col))]]
      (dotimes [_left-idx left-row-count]
        (dotimes [right-idx right-row-count]
          (.appendFrom out-col in-col right-idx))))

    (.read out-rel)))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-rels]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (.add left-rels (.read (doto (rel/->fresh-append-relation allocator)
                                                    (rel/copy-rel-from in-rel)))))))

    (boolean
     (when-not (.isEmpty left-rels)
       (let [left-row-count (transduce (map #(.rowCount ^IReadRelation %)) + left-rels)]
         (.tryAdvance right-cursor
                      (reify Consumer
                        (accept [_ right-rel]
                          (with-open [^IReadRelation out-rel (cross-product left-rels left-row-count right-rel)]
                            (.accept c out-rel)))))))))

  (close [_]
    (when left-rels
      (run! util/try-close left-rels)
      (.clear left-rels))
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defn ->cross-join-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (CrossJoinCursor. allocator left-cursor right-cursor (ArrayList.)))

(deftype BuildPointer [^IReadRelation rel, ^int idx, pointer-or-object])

(defn- build-phase [^BufferAllocator allocator
                    ^IReadRelation build-rel
                    ^List build-rels
                    ^Map join-key->build-pointers
                    ^String build-column-name
                    ^MutableRoaringBitmap pushdown-bloom]
  (let [^IReadRelation build-rel (.read (doto (rel/->fresh-append-relation allocator)
                                          (rel/copy-rel-from build-rel)))
        build-col (.readColumn build-rel build-column-name)]
    (.add build-rels build-rel)
    (dotimes [build-idx (.valueCount build-col)]
      (let [internal-vec (._getInternalVector build-col build-idx)
            internal-idx (._getInternalIndex build-col build-idx)]
        (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec internal-idx))
        (doto ^List (.computeIfAbsent join-key->build-pointers
                                      (.hashCode internal-vec internal-idx)
                                      (reify Function
                                        (apply [_ x]
                                          (ArrayList.))))
          (.add (BuildPointer. build-rel build-idx
                               (util/pointer-or-object internal-vec internal-idx))))))))

(defn- probe-phase ^core2.relation.IReadRelation [^IReadRelation probe-rel
                                                  ^Map join-key->build-pointers
                                                  ^String probe-column-name
                                                  semi-join? anti-join?]
  (when (pos? (.rowCount probe-rel))
    (let [probe-col (.readColumn probe-rel probe-column-name)
          probe-pointer (ArrowBufPointer.)
          matching-build-pointers (ArrayList.)
          matching-probe-idxs (IntStream/builder)]
      (dotimes [probe-idx (.valueCount probe-col)]
        (let [internal-vec (._getInternalVector probe-col probe-idx)
              internal-idx (._getInternalIndex probe-col probe-idx)
              match? (when-let [^List build-pointers (.get join-key->build-pointers (.hashCode internal-vec internal-idx))]
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
                                       (.add matching-build-pointers build-pointer)
                                       (.add matching-probe-idxs probe-idx)
                                       (recur true)))
                             match?))))]
          (cond
            anti-join? (when-not match?
                         (.add matching-probe-idxs probe-idx))
            semi-join? (when match?
                         (.add matching-probe-idxs probe-idx)))))

      (let [out-rel (rel/->indirect-append-relation)]
        (when-not semi-join?
          ;; this one jumps around a bit between build-rels,
          ;; particularly needing to re-read cols and re-gen append-cols for each build-ptr -
          ;; maybe some scope for optimisation
          (doseq [^BuildPointer build-pointer matching-build-pointers
                  :let [^IReadRelation build-rel (.rel build-pointer)]
                  ^IReadColumn in-col (.readColumns build-rel)
                  :when (not= (.getName in-col) probe-column-name)
                  :let [out-col (.appendColumn out-rel (.getName in-col))]]
            (.appendFrom out-col in-col (.idx build-pointer))))

        (let [probe-idxs (.toArray (.build matching-probe-idxs))]
          (doseq [^IReadColumn in-col (.readColumns probe-rel)
                  :let [out-col (.appendColumn out-rel (.getName in-col))]]
            (dotimes [idx (alength probe-idxs)]
              (.appendFrom out-col in-col (aget probe-idxs idx)))))

        (.read out-rel)))))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, ^String build-column-name
                     ^ICursor probe-cursor, ^String probe-column-name
                     ^List build-rels
                     ^Map join-key->build-pointers
                     ^MutableRoaringBitmap pushdown-bloom
                     semi-join? anti-join?]

  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (build-phase allocator in-rel build-rels join-key->build-pointers build-column-name pushdown-bloom))))

    (boolean
     (when (or (not (.isEmpty join-key->build-pointers))
               anti-join?)
       (let [!advanced (atom false)]
         (binding [scan/*column->pushdown-bloom* (if anti-join?
                                                   scan/*column->pushdown-bloom*
                                                   (assoc scan/*column->pushdown-bloom* probe-column-name pushdown-bloom))]
           (while (and (not @!advanced)
                       (.tryAdvance probe-cursor
                                    (reify Consumer
                                      (accept [_ in-rel]
                                        (when-let [out-rel (probe-phase in-rel join-key->build-pointers
                                                                        probe-column-name semi-join? anti-join?)]
                                          (try
                                            (when (pos? (.rowCount out-rel))
                                              (reset! !advanced true)
                                              (.accept c out-rel))
                                            (finally
                                              (util/try-close out-rel))))))))))
         @!advanced))))

  (close [_]
    (.clear pushdown-bloom)
    (run! util/try-close build-rels)
    (.clear build-rels)
    (util/try-close build-cursor)
    (util/try-close probe-cursor)))

(defn ->equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                         ^ICursor left-cursor,
                                         ^String left-column-name,
                                         ^ICursor right-cursor,
                                         ^String right-column-name]
  (JoinCursor. allocator
               left-cursor left-column-name right-cursor right-column-name
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               false false))

(defn ->semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               true false))

(defn ->anti-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               true true))
