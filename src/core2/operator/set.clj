(ns core2.operator.set
  (:require [core2.error :as err]
            [core2.relation :as rel]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.ICursor
           [core2.relation IReadColumn IReadRelation]
           [java.util ArrayList HashSet LinkedList List Set]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(defn- ->union-compatible-schema [^Schema x ^Schema y]
  ;; TODO JH reinstate this check somewhere?
  (letfn [(union-incompatible []
            (err/illegal-arg :union-incompatible
                             {::err/message "Schemas are not union compatible"
                              :left x, :right y}))]
    (let [x-fields (.getFields x)
          y-fields (.getFields y)]
      (when-not (= (count x-fields) (count y-fields))
        (throw (union-incompatible)))
      (Schema. (for [[^Field x-field, ^Field y-field] (map vector x-fields y-fields)
                     :let [x-name (.getName x-field)
                           x-type (Types/getMinorTypeForArrowType
                                   (.getType x-field))
                           y-name (.getName y-field)
                           y-type (Types/getMinorTypeForArrowType
                                   (.getType y-field))]]
                 (cond
                   (not (and (= x-name y-name) (= x-type y-type)))
                   (throw (union-incompatible))

                   (= Types$MinorType/DENSEUNION x-type y-type)
                   (t/->primitive-dense-union-field x-name)

                   :else x-field))))))

(deftype UnionAllCursor [^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (tryAdvance [_ c]
    (boolean
     (or (.tryAdvance left-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IReadRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel))))))

         (.tryAdvance right-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IReadRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel)))))))))

  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

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

(defn- ->set-key [^List cols ^long idx]
  (let [set-key (ArrayList. (count cols))]
    (doseq [^IReadColumn col cols]
      (.add set-key (util/pointer-or-object (._getInternalVector col idx)
                                            (._getInternalIndex col idx))))
    set-key))

(deftype IntersectionCursor [^BufferAllocator allocator
                             ^ICursor left-cursor
                             ^ICursor right-cursor
                             ^Set intersection-set
                             difference?]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (let [^IReadRelation in-rel in-rel
                                 row-count (.rowCount in-rel)]
                             (when (pos? row-count)
                               (let [cols (.readColumns in-rel)]
                                 (dotimes [idx row-count]
                                   (let [set-key (->set-key cols idx)]
                                     (when-not (.contains intersection-set set-key)
                                       (.add intersection-set (copy-set-key allocator set-key)))))))))))

    (boolean
     (when (or difference? (not (.isEmpty intersection-set)))
       (let [!advanced? (atom false)]
         (while (and (not @!advanced?)
                     (.tryAdvance left-cursor
                                  (reify Consumer
                                    (accept [_ in-rel]
                                      (let [^IReadRelation in-rel in-rel
                                            row-count (.rowCount in-rel)]

                                        (when (pos? row-count)
                                          (let [in-cols (.readColumns in-rel)
                                                out-rel (rel/->indirect-append-relation)
                                                row-copier (rel/row-copier out-rel in-rel)]
                                            (dotimes [idx row-count]
                                              (when (cond-> (.contains intersection-set (->set-key in-cols idx))
                                                      difference? not)
                                                (.appendRow row-copier idx)))

                                            (let [out-rel (.read out-rel)]
                                              (when (pos? (.rowCount out-rel))
                                                (.accept c out-rel)))))))))))
         @!advanced?))))

  (close [_]
    (run! release-set-key intersection-set)
    (.clear intersection-set)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(deftype DistinctCursor [^BufferAllocator allocator
                         ^ICursor in-cursor
                         ^Set seen-set]
  ICursor
  (tryAdvance [_ c]
    (let [!advanced? (atom false)]
      (while (and (not @!advanced?)
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IReadRelation in-rel in-rel
                                         row-count (.rowCount in-rel)]
                                     (when (pos? row-count)
                                       (let [cols (.readColumns in-rel)
                                             out-rel (rel/->indirect-append-relation)
                                             row-copier (rel/row-copier out-rel in-rel)]
                                         (dotimes [idx row-count]
                                           (let [k (->set-key cols idx)]
                                             (when-not (.contains seen-set k)
                                               (.add seen-set (copy-set-key allocator k))
                                               (.appendRow row-copier idx))))

                                         (let [out-rel (.read out-rel)]
                                           (when (pos? (.rowCount out-rel))
                                             (.accept c out-rel)))))))))))
      @!advanced?))

  (close [_]
    (run! release-set-key seen-set)
    (.clear seen-set)
    (util/try-close in-cursor)))

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
  (tryAdvance [this c]
    (if-not (or continue? recursive-cursor)
      false

      (let [!advanced? (atom false)
            inner-c (reify Consumer
                      (accept [_ in-rel]
                        (let [^IReadRelation in-rel in-rel]
                          (when (pos? (.rowCount in-rel))
                            (let [cols (.readColumns in-rel)
                                  out-rel (rel/->fresh-append-relation allocator)
                                  row-copier (rel/row-copier out-rel in-rel)]
                              (dotimes [idx (.rowCount in-rel)]
                                (let [k (->set-key cols idx)]
                                  (when-not (.contains fixpoint-set k)
                                    (.add fixpoint-set (copy-set-key allocator k))
                                    (.appendRow row-copier idx))))

                              (let [out-rel (.read out-rel)]
                                (when (pos? (.rowCount out-rel))
                                  (.add rels out-rel)
                                  (.accept c out-rel)
                                  (set! (.continue? this) true)
                                  (reset! !advanced? true))))))))]

        (.tryAdvance base-cursor inner-c)

        (or @!advanced?
            (do
              (while (and (not @!advanced?) continue?)
                (when-let [recursive-cursor (or recursive-cursor
                                                (when continue?
                                                  (set! (.continue? this) false)
                                                  (let [cursor (.createCursor recursive-cursor-factory
                                                                              (->fixpoint-cursor-factory (vec rels) incremental?))]
                                                    (when incremental? (.clear rels))
                                                    (set! (.recursive-cursor this) cursor)
                                                    cursor)))]

                  (while (and (not @!advanced?)
                              (let [more? (.tryAdvance recursive-cursor inner-c)]
                                (when-not more?
                                  (util/try-close recursive-cursor)
                                  (set! (.recursive-cursor this) nil))
                                more?)))))
              @!advanced?)))))

  (close [_]
    (util/try-close recursive-cursor)
    (run! util/try-close rels)
    (run! release-set-key fixpoint-set)
    (.clear fixpoint-set)
    (util/try-close base-cursor)))

(defn ->union-all-cursor ^core2.ICursor [^ICursor left-cursor, ^ICursor right-cursor]
  (UnionAllCursor. left-cursor right-cursor))

(defn ->difference-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) true))

(defn ->intersection-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor left-cursor, ^ICursor right-cursor]
  (IntersectionCursor. allocator left-cursor right-cursor (HashSet.) false))

(defn ->distinct-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor]
  (DistinctCursor. allocator in-cursor (HashSet.)))

(defn ->fixpoint-cursor ^core2.ICursor [^BufferAllocator allocator,
                                        ^ICursor base-cursor
                                        ^IFixpointCursorFactory recursive-cursor-factory
                                        incremental?]
  (FixpointCursor. allocator base-cursor recursive-cursor-factory
                   (HashSet.) (LinkedList.) incremental? nil true))
