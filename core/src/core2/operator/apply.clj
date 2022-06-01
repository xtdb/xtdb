(ns core2.operator.apply
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           (core2 ICursor)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector NullVector)))

(defmethod lp/ra-expr :apply [_]
  (s/cat :op #{:apply}
         :mode #{:cross-join, :left-outer-join, :semi-join, :anti-join}
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true)
         :independent-relation ::lp/ra-expression
         :dependent-relation ::lp/ra-expression))

(definterface IDependentCursorFactory
  (^core2.ICursor openDependentCursor [^core2.vector.IIndirectRelation inRelation, ^int idx]))

(definterface ModeStrategy
  (^void accept [^core2.ICursor dependentCursor
                 ^core2.vector.IRelationWriter dependentOutWriter
                 ^java.util.stream.IntStream$Builder idxs
                 ^int inIdx]))

(defn ->mode-strategy [mode dependent-col-names]
  (case mode
    :cross-join
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
            (doseq [^String col-name (map name dependent-col-names)]
              (vw/append-vec (.writerForName dep-out-writer col-name)
                             (iv/->direct-vec (doto (NullVector. col-name)
                                                (.setValueCount 1)))))))))

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
            (.add idxs in-idx)))))))

(deftype ApplyCursor [^BufferAllocator allocator
                      ^ModeStrategy mode-strategy
                      ^ICursor independent-cursor
                      ^IDependentCursorFactory dependent-cursor-factory]
  ICursor
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

(defmethod lp/emit-expr :apply [{:keys [mode columns independent-relation dependent-relation]} args]
  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?

  (lp/unary-expr independent-relation args
    (fn [independent-col-types]
      (let [dependent-args (-> args
                               (update :param-names
                                       (fnil into #{})
                                       (map second)
                                       columns))
            {dependent-col-types :col-types, ->dependent-cursor :->cursor} (lp/emit-expr dependent-relation dependent-args)]

        {:col-types (case mode
                      (:cross-join :left-outer-join) (merge-with types/merge-col-types independent-col-types dependent-col-types)
                      (:semi-join :anti-join) independent-col-types)
         :->cursor (fn [{:keys [allocator] :as query-opts} independent-cursor]
                     (let [dependent-cursor-factory
                           (reify IDependentCursorFactory
                             (openDependentCursor [_ in-rel idx]
                               (let [query-opts (-> query-opts
                                                    (update :params
                                                            (fnil into {})
                                                            (for [[ik dk] columns]
                                                              (let [iv (.vectorForName in-rel (name ik))]
                                                                (MapEntry/create dk (types/get-object (.getVector iv) (.getIndex iv idx)))))))]
                                 (->dependent-cursor query-opts))))]

                       (ApplyCursor. allocator (->mode-strategy mode (set (keys dependent-col-types))) independent-cursor dependent-cursor-factory)))}))))
