(ns core2.operator.join
  (:require [core2.bloom :as bloom]
            [core2.operator.scan :as scan]
            [core2.relation :as rel]
            [core2.util :as util])
  (:import core2.ICursor
           [core2.relation IAppendRelation IReadColumn IReadRelation IRowAppender]
           [java.util ArrayList HashMap Iterator List Map]
           [java.util.function Consumer Function]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           org.roaringbitmap.buffer.MutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn- cross-product ^core2.relation.IReadRelation [^IReadRelation left-rel, ^IReadRelation right-rel]
  (let [left-row-count (.rowCount left-rel)
        right-row-count (.rowCount right-rel)
        row-count (* left-row-count right-row-count)]
    (rel/->read-relation (concat (rel/select left-rel
                                             (let [idxs (int-array row-count)]
                                               (dotimes [idx row-count]
                                                 (aset idxs idx ^long (quot idx right-row-count)))
                                               idxs))

                                 (rel/select right-rel
                                             (let [idxs (int-array row-count)]
                                               (dotimes [idx row-count]
                                                 (aset idxs idx ^long (rem idx right-row-count)))
                                               idxs))))))

(deftype CrossJoinCursor [^BufferAllocator allocator
                          ^ICursor left-cursor
                          ^ICursor right-cursor
                          ^List left-rels
                          ^:unsynchronized-mutable ^Iterator left-rel-iterator
                          ^:unsynchronized-mutable ^IReadRelation right-rel]
  ICursor
  (tryAdvance [this c]
    (.forEachRemaining left-cursor
                       (reify Consumer
                         (accept [_ left-rel]
                           (.add left-rels (rel/copy left-rel allocator)))))

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
                                                       (set! (.right-rel this) (rel/copy right-rel allocator))
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

(deftype BuildPointer [^List #_<IRowAppender> row-appenders, ^int idx, pointer-or-object])

(defn- build-phase [^IAppendRelation append-rel
                    ^IReadRelation build-rel
                    ^String build-column-name
                    ^String probe-column-name
                    ^Map join-key->build-pointers
                    ^MutableRoaringBitmap pushdown-bloom
                    semi-join?]
  (let [row-appenders (when-not semi-join?
                        (vec (for [^IReadColumn col build-rel
                                   :let [col-name (.getName col)]
                                   :when (not= col-name probe-column-name)]
                               (.rowAppender (.appendColumn append-rel col-name) col))))
        build-col (.readColumn build-rel build-column-name)
        internal-vec (.getVector build-col)]
    (dotimes [build-idx (.getValueCount build-col)]
      (let [idx (.getIndex build-col build-idx)]
        (.add pushdown-bloom ^ints (bloom/bloom-hashes internal-vec idx))
        (doto ^List (.computeIfAbsent join-key->build-pointers
                                      (.hashCode internal-vec idx)
                                      (reify Function
                                        (apply [_ x]
                                          (ArrayList.))))
          (.add (BuildPointer. row-appenders build-idx
                               (util/pointer-or-object internal-vec idx))))))))

(defn- probe-phase ^core2.relation.IReadRelation [^IAppendRelation append-rel
                                                  ^IReadRelation probe-rel
                                                  ^Map join-key->build-pointers
                                                  ^String probe-column-name
                                                  semi-join? anti-join?]
  (when (pos? (.rowCount probe-rel))
    (let [probe-row-appenders (vec (for [^IReadColumn col probe-rel]
                                     (.rowAppender (.appendColumn append-rel (.getName col)) col)))
          probe-col (.readColumn probe-rel probe-column-name)
          probe-pointer (ArrowBufPointer.)
          matching-build-pointers (ArrayList.)
          matching-probe-idxs (IntStream/builder)
          internal-vec (.getVector probe-col)]
      (dotimes [probe-idx (.getValueCount probe-col)]
        (let [internal-idx (.getIndex probe-col probe-idx)
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

      (when-not semi-join?
        (doseq [^BuildPointer build-pointer matching-build-pointers
                ^IRowAppender row-appender (.row-appenders build-pointer)]
          (.appendRow row-appender (.idx build-pointer))))

      (let [probe-idxs (.toArray (.build matching-probe-idxs))]
        (doseq [^IRowAppender row-appender probe-row-appenders]
          (dotimes [idx (alength probe-idxs)]
            (.appendRow row-appender (aget probe-idxs idx))))))))

(deftype JoinCursor [^BufferAllocator allocator
                     ^ICursor build-cursor, ^String build-column-name
                     ^ICursor probe-cursor, ^String probe-column-name
                     ^IAppendRelation append-rel
                     ^List build-rels
                     ^Map join-key->build-pointers
                     ^MutableRoaringBitmap pushdown-bloom
                     semi-join? anti-join?]

  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining build-cursor
                       (reify Consumer
                         (accept [_ build-rel]
                           (let [build-rel (rel/copy build-rel allocator)]
                             (.add build-rels build-rel)
                             (build-phase append-rel build-rel
                                          build-column-name probe-column-name
                                          join-key->build-pointers pushdown-bloom
                                          semi-join?)))))

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
                                      (accept [_ probe-rel]
                                        (probe-phase append-rel probe-rel join-key->build-pointers
                                                     probe-column-name semi-join? anti-join?)
                                        (let [out-rel (.read append-rel)]
                                          (try
                                            (when (pos? (.rowCount out-rel))
                                              (reset! !advanced true)
                                              (.accept c out-rel))
                                            (finally
                                              (.clear append-rel)
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
               (rel/->append-relation allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               false false))

(defn ->semi-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (rel/->append-relation allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               true false))

(defn ->anti-equi-join-cursor ^core2.ICursor [^BufferAllocator allocator,
                                              ^ICursor left-cursor,
                                              ^String left-column-name,
                                              ^ICursor right-cursor,
                                              ^String right-column-name]
  (JoinCursor. allocator
               right-cursor right-column-name left-cursor left-column-name
               (rel/->append-relation allocator)
               (ArrayList.) (HashMap.) (MutableRoaringBitmap.)
               true true))
