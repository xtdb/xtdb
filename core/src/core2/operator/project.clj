(ns core2.operator.project
  (:require [clojure.spec.alpha :as s]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.operator.IProjectionSpec
           java.util.function.Consumer
           java.util.ArrayList
           java.util.List
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.BigIntVector))

(s/def ::append-columns? boolean?)

(defmethod lp/ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :opts (s/? (s/keys :req-un [::append-columns?]))
         :projections (s/coll-of (s/or :column ::lp/column
                                       :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :extend ::lp/column-expression)
                                 :min-count 1)
         :relation ::lp/ra-expression))

(defmethod lp/ra-expr :map [_]
  (s/cat :op #{:ⲭ :chi :map}
         :projections (s/coll-of (s/or :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :extend ::lp/column-expression)
                                 :min-count 1)
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defrecord IdentityProjectionSpec [col-name col-type]
  IProjectionSpec
  (getColumnName [_] col-name)
  (getColumnType [_] col-type)
  (project [_ _allocator in-rel _params]
    (.vectorForName in-rel (name col-name))))

(defn ->identity-projection-spec ^core2.operator.IProjectionSpec [col-name col-type]
  (->IdentityProjectionSpec col-name col-type))

(defn ->row-number-projection-spec ^core2.operator.IProjectionSpec [col-name]
  (let [row-num (long-array [1])]
    (reify IProjectionSpec
      (getColumnName [_] col-name)
      (getColumnType [_] :i64)
      (project [_ allocator in-rel _params]
        (let [out-vec (BigIntVector. (name col-name) allocator)
              start-row-num (aget row-num 0)
              row-count (.rowCount in-rel)]
          (try
            (.setValueCount out-vec row-count)
            (dotimes [idx row-count]
              (.set out-vec idx (+ idx start-row-num)))
            (aset row-num 0 (+ start-row-num row-count))
            (iv/->direct-vec out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<IProjectionSpec> projection-specs
                        params]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (reify Consumer
                   (accept [_ read-rel]
                     (let [close-cols (ArrayList.)
                           out-cols (ArrayList.)]
                       (try
                         (doseq [^IProjectionSpec projection-spec projection-specs]
                           (let [out-col (.project projection-spec allocator read-rel params)]
                             (when-not (instance? IdentityProjectionSpec projection-spec)
                               (.add close-cols out-col))
                             (.add out-cols out-col)))

                         (.accept c (iv/->indirect-rel out-cols))

                         (finally
                           (run! util/try-close close-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->project-cursor [{:keys [allocator, params] :as _opts} in-cursor projection-specs]
  (->ProjectCursor allocator in-cursor projection-specs params))

(defmethod lp/emit-expr :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [param-types] :as args}]
  (lp/unary-expr relation args
    (fn [inner-col-types]
      (let [projection-specs (concat (when append-columns?
                                       (for [[col-name col-type] inner-col-types]
                                         (->identity-projection-spec col-name col-type)))
                                     (for [[p-type arg] projections]
                                       (case p-type
                                         :column (->identity-projection-spec arg (get inner-col-types arg))
                                         :row-number-column (let [[col-name _form] (first arg)]
                                                              (->row-number-projection-spec col-name))
                                         :extend (let [[col-name form] (first arg)]
                                                   (expr/->expression-projection-spec col-name form
                                                                                      {:col-names (set (keys inner-col-types))
                                                                                       :param-types param-types})))))]
        {:col-types (->> projection-specs
                         (into {} (map (juxt #(.getColumnName ^IProjectionSpec %)
                                             #(.getColumnType ^IProjectionSpec %)))))
         :->cursor (fn [opts in-cursor] (->project-cursor opts in-cursor projection-specs))}))))

(defmethod lp/emit-expr :map [op args]
  (lp/emit-expr (assoc op :op :project :opts {:append-columns? true}) args))
