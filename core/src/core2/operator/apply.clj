(ns core2.operator.apply
  (:require [clojure.set :as set]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)))

(definterface IDependentCursorFactory
  (^java.util.Set getColumnNames [])
  (^core2.ICursor openDependentCursor [^core2.vector.IIndirectRelation inRelation, ^int idx]))

(definterface ModeStrategy
  (^void accept [^core2.ICursor dependentCursor
                 ^core2.vector.IRelationWriter dependentOutWriter
                 ^java.util.stream.IntStream$Builder idxs
                 ^int inIdx]))

(def mode-strategies
  {:cross-join
   (reify ModeStrategy
     (accept [_ dep-cursor dep-out-writer idxs in-idx]
       (while (.tryAdvance dep-cursor
                           (reify Consumer
                             (accept [_ dep-rel]
                               (let [^IIndirectRelation dep-rel dep-rel]
                                 (vw/append-rel dep-out-writer dep-rel)

                                 (dotimes [_ (.rowCount dep-rel)]
                                   (.add idxs in-idx)))))))))

   :left-outer-join
   (reify ModeStrategy
     (accept [_ dep-cursor dep-out-writer idxs in-idx]
       (let [match? (boolean-array [false])]
         (while (.tryAdvance dep-cursor
                             (reify Consumer
                               (accept [_ dep-rel]
                                 (let [^IIndirectRelation dep-rel dep-rel]
                                   (when (pos? (.rowCount dep-rel))
                                     (aset match? 0 true)
                                     (vw/append-rel dep-out-writer dep-rel)

                                     (dotimes [_ (.rowCount dep-rel)]
                                       (.add idxs in-idx))))))))
         (when-not (aget match? 0)
           (.add idxs in-idx)
           (doseq [col-name (.getColumnNames dep-cursor)]
             (doto (-> (.writerForName dep-out-writer (name col-name))
                       (vw/->null-row-copier))
               (.copyRow -1)))))))

   :semi-join
   (reify ModeStrategy
     (accept [_ dep-cursor _dep-out-writer idxs in-idx]
       (let [match? (boolean-array [false])]
         (while (and (not (aget match? 0))
                     (.tryAdvance dep-cursor
                                  (reify Consumer
                                    (accept [_ dep-rel]
                                      (let [^IIndirectRelation dep-rel dep-rel]
                                        (when (pos? (.rowCount dep-rel))
                                          (aset match? 0 true)
                                          (.add idxs in-idx)))))))))))

   :anti-join
   (reify ModeStrategy
     (accept [_ dep-cursor _dep-out-writer idxs in-idx]
       (let [match? (boolean-array [false])]
         (while (and (not (aget match? 0))
                     (.tryAdvance dep-cursor
                                  (reify Consumer
                                    (accept [_ dep-rel]
                                      (let [^IIndirectRelation dep-rel dep-rel]
                                        (when (pos? (.rowCount dep-rel))
                                          (aset match? 0 true))))))))
         (when-not (aget match? 0)
           (.add idxs in-idx)))))})

(deftype ApplyCursor [^BufferAllocator allocator
                      ^ModeStrategy mode-strategy
                      ^ICursor independent-cursor
                      ^IDependentCursorFactory dependent-cursor-factory]
  ICursor
  (getColumnNames [_]
    (set/union (.getColumnNames independent-cursor)
               (.getColumnNames dependent-cursor-factory)))

  (tryAdvance [_ c]
    (.tryAdvance independent-cursor
                 (reify Consumer
                   (accept [_ in-rel]
                     (let [^IIndirectRelation in-rel in-rel
                           idxs (IntStream/builder)]
                       (with-open [dep-out-writer (vw/->rel-writer allocator)]
                         (dotimes [in-idx (.rowCount in-rel)]
                           (with-open [dep-cursor (.openDependentCursor dependent-cursor-factory
                                                                        in-rel in-idx)]
                             (.accept mode-strategy dep-cursor dep-out-writer idxs in-idx)))

                         (let [idxs (.toArray (.build idxs))]
                           (.accept c (iv/->indirect-rel (concat (for [^IIndirectVector col in-rel]
                                                                   (.select col idxs))
                                                                 (for [^IVectorWriter vec-writer dep-out-writer]
                                                                   (iv/->direct-vec (.getVector vec-writer)))))))))))))

  (close [_]
    (util/try-close independent-cursor)))

(defn ->apply-operator [allocator mode independent-cursor dependent-cursor-factory]
  (ApplyCursor. allocator (get mode-strategies mode) independent-cursor dependent-cursor-factory))
