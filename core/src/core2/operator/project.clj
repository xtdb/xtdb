(ns core2.operator.project
  (:require [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.operator.IProjectionSpec
           java.util.function.Consumer
           java.util.LinkedList
           java.util.List
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.BigIntVector))

(set! *unchecked-math* :warn-on-boxed)

(deftype IdentityProjectionSpec [^String col-name]
  )

(defn ->identity-projection-spec ^core2.operator.IProjectionSpec [^String col-name]
  (reify IProjectionSpec
    (getColumnName [_] col-name)
    (project [_ _allocator in-rel]
      (.vectorForName in-rel col-name))))

(defn ->row-number-projection-spec ^core2.operator.IProjectionSpec [^String col-name]
  (let [row-num (long-array [1])]
    (reify IProjectionSpec
      (getColumnName [_] col-name)
      (project [_ allocator in-rel]
        (let [out-vec (BigIntVector. col-name allocator)
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
                        ^List #_<IProjectionSpec> projection-specs]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (reify Consumer
                   (accept [_ read-rel]
                     (let [out-cols (LinkedList.)]
                       (try
                         (doseq [^IProjectionSpec projection-spec projection-specs]
                           (let [out-col (.project projection-spec allocator read-rel)]
                             (.add out-cols out-col)))

                         (.accept c (iv/->indirect-rel out-cols))

                         (finally
                           (run! util/try-close out-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->project-cursor ^core2.ICursor [projection-specs {:keys [allocator]} in-cursor]
  (ProjectCursor. allocator in-cursor projection-specs))

(defmethod lp/emit-expr :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [params] :as args}]
  (lp/unary-expr relation args
    (fn [inner-col-names]
      (let [projection-specs (concat (when append-columns?
                                       (for [col-name inner-col-names]
                                         (->identity-projection-spec (name col-name))))
                                     (for [[p-type arg] projections]
                                       (case p-type
                                         :column (->identity-projection-spec (name arg))
                                         :row-number-column (let [[col-name _form] (first arg)]
                                                              (->row-number-projection-spec (name col-name)))
                                         :extend (let [[col-name form] (first arg)]
                                                   (expr/->expression-projection-spec (name col-name) form (into #{} (map symbol) inner-col-names) params)))))]
        {:col-names (->> projection-specs
                         (into #{} (map #(.getColumnName ^IProjectionSpec %))))
         :->cursor (partial ->project-cursor projection-specs)}))))

(defmethod lp/emit-expr :map [op args]
  (lp/emit-expr (assoc op :op :project :opts {:append-columns? true}) args))
