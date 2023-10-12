(ns xtdb.operator.apply
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector NullVector)
           org.apache.arrow.vector.types.pojo.FieldType
           (xtdb ICursor)
           (xtdb.vector IVectorReader RelationReader)))

(defmethod lp/ra-expr :apply [_]
  (s/cat :op #{:apply}
         :mode (s/or :mark-join (s/map-of #{:mark-join} ::lp/column-expression, :count 1, :conform-keys true)
                     :otherwise #{:cross-join, :left-outer-join, :semi-join, :anti-join, :single-join})
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true)
         :independent-relation ::lp/ra-expression
         :dependent-relation ::lp/ra-expression))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IDependentCursorFactory
  (^xtdb.ICursor openDependentCursor [^xtdb.vector.RelationReader inRelation, ^int idx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ModeStrategy
  (^void accept [^xtdb.ICursor dependentCursor
                 ^xtdb.vector.IRelationWriter dependentOutWriter
                 ^java.util.stream.IntStream$Builder idxs
                 ^int inIdx]))

(defn ->mode-strategy [mode dependent-col-types]
  (zmatch mode
    [:mark-join mark-spec]
    (let [[col-name _expr] (first (:mark-join mark-spec))]
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (let [out-writer (.colWriter dep-out-writer (name col-name) (FieldType/nullable #xt.arrow/type :bool))]
            (.add idxs in-idx)
            (let [!match (int-array [-1])]
              (while (and (not (== 1 (aget !match 0)))
                          (.tryAdvance dep-cursor
                                       (reify Consumer
                                         (accept [_ dep-rel]
                                           (let [match-vec (.readerForName ^RelationReader dep-rel "_expr")
                                                 match-rdr (.polyReader match-vec [:union #{:null :bool}])]
                                             ;; HACK: if the iteration order changes I'd like to know about it :)
                                             (case (vec #{:null :bool})
                                               [:null :bool]
                                               (dotimes [idx (.valueCount match-vec)]
                                                 (case (.read match-rdr idx)
                                                   0 (aset !match 0 (max (aget !match 0) 0))
                                                   1 (aset !match 0 (max (aget !match 0) (if (.readBoolean match-rdr) 1 -1))))))))))))
              (let [match (aget !match 0)]
                (if (zero? match)
                  (.writeNull out-writer nil)
                  (.writeBoolean out-writer (== 1 match)))))))))

    [:otherwise simple-mode]
    (case simple-mode
      :cross-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.colWriter dep-out-writer (name col-name) (.getFieldType (types/col-type->field (name col-name) col-type))))

          (.forEachRemaining dep-cursor
                             (reify Consumer
                               (accept [_ dep-rel]
                                 (let [^RelationReader dep-rel dep-rel]
                                   (vw/append-rel dep-out-writer dep-rel)

                                   (dotimes [_ (.rowCount dep-rel)]
                                     (.add idxs in-idx))))))))

      :left-outer-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.colWriter dep-out-writer (name col-name) (.getFieldType (types/col-type->field (name col-name) col-type))))

          (let [match? (boolean-array [false])]
            (.forEachRemaining dep-cursor
                               (reify Consumer
                                 (accept [_ dep-rel]
                                   (let [^RelationReader dep-rel dep-rel]
                                     (when (pos? (.rowCount dep-rel))
                                       (aset match? 0 true)
                                       (vw/append-rel dep-out-writer dep-rel)

                                       (dotimes [_ (.rowCount dep-rel)]
                                         (.add idxs in-idx)))))))

            (when-not (aget match? 0)
              (.add idxs in-idx)
              (doseq [[col-name col-type] dependent-col-types]
                (vw/append-vec (.colWriter dep-out-writer (name col-name) (.getFieldType (types/col-type->field (name col-name) col-type)))
                               (vr/vec->reader (doto (NullVector. (name col-name))
                                                 (.setValueCount 1)))))))))

      :semi-join
      (reify ModeStrategy
        (accept [_ dep-cursor _dep-out-writer idxs in-idx]
          (let [match? (boolean-array [false])]
            (while (and (not (aget match? 0))
                        (.tryAdvance dep-cursor
                                     (reify Consumer
                                       (accept [_ dep-rel]
                                         (let [^RelationReader dep-rel dep-rel]
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
                                         (let [^RelationReader dep-rel dep-rel]
                                           (when (pos? (.rowCount dep-rel))
                                             (aset match? 0 true))))))))
            (when-not (aget match? 0)
              (.add idxs in-idx)))))

      :single-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.colWriter dep-out-writer (name col-name) (.getFieldType (types/col-type->field (name col-name) col-type))))

          (let [match? (boolean-array [false])]
            (.forEachRemaining dep-cursor
                               (reify Consumer
                                 (accept [_ dep-rel]
                                   (let [^RelationReader dep-rel dep-rel
                                         row-count (.rowCount dep-rel)]
                                     (cond
                                       (zero? row-count) nil

                                       (> (+ row-count (if (aget match? 0) 1 0)) 1)
                                       (throw (err/runtime-err :xtdb.single-join/cardinality-violation
                                                               {::err/message "cardinality violation"}))

                                       :else
                                       (do
                                         (aset match? 0 true)
                                         (.add idxs in-idx)
                                         (vw/append-rel dep-out-writer dep-rel)))))))

            (when-not (aget match? 0)
              (.add idxs in-idx)
              (doseq [[col-name col-type] dependent-col-types]
                (vw/append-vec (.colWriter dep-out-writer (name col-name) (.getFieldType (types/col-type->field (name col-name) col-type)))
                               (vr/vec->reader (doto (NullVector. (name col-name))
                                                 (.setValueCount 1))))))))))))

(deftype ApplyCursor [^BufferAllocator allocator
                      ^ModeStrategy mode-strategy
                      ^ICursor independent-cursor
                      ^IDependentCursorFactory dependent-cursor-factory]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance independent-cursor
                 (reify Consumer
                   (accept [_ in-rel]
                     (let [^RelationReader in-rel in-rel
                           idxs (IntStream/builder)]

                       (with-open [dep-out-writer (vw/->rel-writer allocator)]
                         (dotimes [in-idx (.rowCount in-rel)]
                           (with-open [dep-cursor (.openDependentCursor dependent-cursor-factory
                                                                        in-rel in-idx)]
                             (.accept mode-strategy dep-cursor dep-out-writer idxs in-idx)))

                         (let [idxs (.toArray (.build idxs))]
                           (.accept c (vr/rel-reader (concat (for [^IVectorReader col in-rel]
                                                               (.select col idxs))
                                                             (vw/rel-wtr->rdr dep-out-writer))
                                                     (alength idxs))))))))))

  (close [_]
    (util/try-close independent-cursor)))

(defmethod lp/emit-expr :apply [{:keys [mode columns independent-relation dependent-relation]} args]

  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?

  (lp/unary-expr (lp/emit-expr independent-relation args)
    (fn [independent-col-types]
      (let [{:keys [param-types] :as dependent-args} (-> args
                                                         (update :param-types
                                                                 (fnil into {})
                                                                 (map (fn [[ik dk]]
                                                                        (if-let [col-type (get independent-col-types ik)]
                                                                          [dk col-type]
                                                                          (throw
                                                                            (err/illegal-arg
                                                                              :xtdb.apply/missing-column
                                                                              {::err/message (str "Column missing from independent relation: " ik)
                                                                               :column ik})))))
                                                                 columns))
            {dependent-col-types :col-types, ->dependent-cursor :->cursor} (lp/emit-expr dependent-relation dependent-args)
            out-dependent-col-types (zmatch mode
                                      [:mark-join mark-spec]
                                      (let [[col-name _expr] (first (:mark-join mark-spec))]
                                        {col-name [:union #{:null :bool}]})

                                      [:otherwise simple-mode]
                                      (case simple-mode
                                        :cross-join dependent-col-types

                                        (:left-outer-join :single-join)
                                        (-> dependent-col-types types/with-nullable-cols)

                                        (:semi-join :anti-join) {}))]

        {:col-types (merge-with types/merge-col-types independent-col-types out-dependent-col-types)

         :->cursor (let [mode-strat (->mode-strategy mode out-dependent-col-types)

                         open-dependent-cursor
                         (zmatch mode
                           [:mark-join mark-spec]
                           (let [[_col-name expr] (first (:mark-join mark-spec))
                                 projection-spec (expr/->expression-projection-spec "_expr" expr
                                                                                    {:col-types dependent-col-types
                                                                                     :param-types param-types})]
                             (fn [{:keys [allocator params] :as query-opts}]
                               (let [^ICursor dep-cursor (->dependent-cursor query-opts)]
                                 (reify ICursor
                                   (tryAdvance [_ c]
                                     (.tryAdvance dep-cursor (reify Consumer
                                                               (accept [_ in-rel]
                                                                 (with-open [match-vec (.project projection-spec allocator in-rel params)]
                                                                   (.accept c (vr/rel-reader [match-vec])))))))

                                   (close [_] (.close dep-cursor))))))

                           [:otherwise _] ->dependent-cursor)]

                     (fn [{:keys [allocator] :as query-opts} independent-cursor]
                       (ApplyCursor. allocator mode-strat independent-cursor
                                     (reify IDependentCursorFactory
                                       (openDependentCursor [_this in-rel idx]
                                         (open-dependent-cursor (-> query-opts
                                                                    (update :params
                                                                            (fn [params]
                                                                              (vr/rel-reader (concat params
                                                                                                     (for [[ik dk] columns]
                                                                                                       (-> (.readerForName in-rel (name ik))
                                                                                                           (.select (int-array [idx]))
                                                                                                           (.withName (name dk)))))
                                                                                             1))))))))))}))))
