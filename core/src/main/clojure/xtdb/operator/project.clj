(ns xtdb.operator.project
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import java.time.Clock
           java.util.ArrayList
           java.util.List
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.BigIntVector
           xtdb.ICursor
           xtdb.operator.IProjectionSpec
           xtdb.vector.RelationReader))

(s/def ::append-columns? boolean?)

(defmethod lp/ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :opts (s/? (s/keys :req-un [::append-columns?]))
         :projections (s/coll-of (s/or :column ::lp/column
                                       :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       ;; don't do this for params, because they aren't real cols
                                       ;; the EE handles these through `:extend`
                                       :rename (s/map-of ::lp/column (s/and ::lp/column #(not (str/starts-with? (name %) "?")))
                                                         :conform-keys true, :count 1)
                                       :extend ::lp/column-expression))
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
    (.readerForName in-rel (str col-name))))

(defn ->identity-projection-spec ^xtdb.operator.IProjectionSpec [col-name col-type]
  (->IdentityProjectionSpec col-name col-type))

(defn ->row-number-projection-spec ^xtdb.operator.IProjectionSpec [col-name]
  (let [row-num (long-array [1])]
    (reify IProjectionSpec
      (getColumnName [_] col-name)
      (getColumnType [_] :i64)
      (project [_ allocator in-rel _params]
        (util/with-close-on-catch [row-num-wtr (vw/->writer (BigIntVector. (str col-name) allocator))]
          (let [start-row-num (aget row-num 0)
                row-count (.rowCount in-rel)]
            (dotimes [idx row-count]
              (.writeLong row-num-wtr (+ idx start-row-num)))
            (aset row-num 0 (+ start-row-num row-count))
            (vw/vec-wtr->rdr row-num-wtr)))))))

(defrecord RenameProjectionSpec [to-name from-name col-type]
  IProjectionSpec
  (getColumnName [_] to-name)
  (getColumnType [_] col-type)
  (project [_ _allocator in-rel _params]
    (-> (.readerForName in-rel (str from-name))
        (.withName (str to-name)))))

(defn ->rename-projection-spec ^xtdb.operator.IProjectionSpec [to-name from-name col-type]
  (->RenameProjectionSpec to-name from-name col-type))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<IProjectionSpec> projection-specs
                        ^Clock clock
                        params]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (reify Consumer
                   (accept [_ read-rel]
                     (let [^RelationReader read-rel read-rel
                           close-cols (ArrayList.)
                           out-cols (ArrayList.)]
                       (try
                         (doseq [^IProjectionSpec projection-spec projection-specs]
                           (let [out-col (.project projection-spec allocator read-rel params)]
                             (when-not (or (instance? IdentityProjectionSpec projection-spec)
                                           (instance? RenameProjectionSpec projection-spec))
                               (.add close-cols out-col))
                             (.add out-cols out-col)))

                         (.accept c (vr/rel-reader out-cols (.rowCount read-rel)))

                         (finally
                           (run! util/try-close close-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->project-cursor [{:keys [allocator clock params]} in-cursor projection-specs]
  (->ProjectCursor allocator in-cursor projection-specs clock params))

(defmethod lp/emit-expr :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [param-types] :as args}]
  (let [emmited-child-relation (lp/emit-expr relation args)]
    (lp/unary-expr
      emmited-child-relation
      (fn [inner-col-types]
        (let [projection-specs (concat (when append-columns?
                                         (for [[col-name col-type] inner-col-types]
                                           (->identity-projection-spec col-name col-type)))
                                       (for [[p-type arg] projections]
                                         (case p-type
                                           :column (->identity-projection-spec arg (get inner-col-types arg))
                                           :row-number-column (let [[col-name _form] (first arg)]
                                                                (->row-number-projection-spec col-name))
                                           :rename (let [[to-name from-name] (first arg)]
                                                     (->rename-projection-spec to-name from-name (get inner-col-types from-name)))
                                           :extend (let [[col-name form] (first arg)]
                                                     (expr/->expression-projection-spec col-name form
                                                                                        {:col-types inner-col-types
                                                                                         :param-types param-types})))))]
          {:col-types (->> projection-specs
                           (into {} (map (juxt #(.getColumnName ^IProjectionSpec %)
                                               #(.getColumnType ^IProjectionSpec %)))))
           :stats (:stats emmited-child-relation)
           :->cursor (fn [opts in-cursor] (->project-cursor opts in-cursor projection-specs))})))))

(defmethod lp/emit-expr :map [op args]
  (lp/emit-expr (assoc op :op :project :opts {:append-columns? true}) args))
