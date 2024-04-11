(ns xtdb.operator.project
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import java.time.Clock
           java.util.ArrayList
           java.util.function.Consumer
           java.util.List
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.complex.StructVector
           (org.apache.arrow.vector.types.pojo Field FieldType)
           xtdb.ICursor
           xtdb.operator.IProjectionSpec
           xtdb.vector.IVectorReader
           xtdb.vector.RelationReader))

(s/def ::append-columns? boolean?)

(defmethod lp/ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :opts (s/? (s/keys :req-un [::append-columns?]))
         :projections (s/coll-of (s/or :column ::lp/column
                                       :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
                                       ;; don't do this for params, because they aren't real cols
                                       ;; the EE handles these through `:extend`
                                       :rename (s/map-of ::lp/column (s/and ::lp/column #(not (str/starts-with? (name %) "?")))
                                                         :conform-keys true, :count 1)
                                       :extend ::lp/column-expression))
         :relation ::lp/ra-expression))

(defmethod lp/ra-expr :map [_]
  (s/cat :op #{:ⲭ :chi :map}
         :projections (s/coll-of (s/or :row-number-column (s/map-of ::lp/column #{'(row-number)}, :conform-keys true, :count 1)
                                       :star (s/map-of ::lp/column #{'*}, :conform-keys true, :count 1)
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

(defn ->identity-projection-spec ^xtdb.operator.IProjectionSpec [col-name field]
  (->IdentityProjectionSpec col-name (types/field->col-type field)))

(defn ->row-number-projection-spec ^xtdb.operator.IProjectionSpec [col-name]
  (let [row-num (long-array [1])]
    (reify IProjectionSpec
      (getColumnName [_] col-name)
      (getColumnType [_] :i64)
      (project [_ allocator in-rel _params]
        (util/with-close-on-catch [row-num-wtr (vw/->writer (BigIntVector. (str col-name) (FieldType/notNullable #xt.arrow/type :i64) allocator))]
          (let [start-row-num (aget row-num 0)
                row-count (.rowCount in-rel)]
            (dotimes [idx row-count]
              (.writeLong row-num-wtr (+ idx start-row-num)))
            (aset row-num 0 (+ start-row-num row-count))
            (vw/vec-wtr->rdr row-num-wtr)))))))

(defn ->star-projection-spec ^xtdb.operator.IProjectionSpec [col-name col-type]
  (reify IProjectionSpec
    (getColumnName [_] col-name)
    (getColumnType [_] col-type)
    (project [_ allocator in-rel _params]
      (let [row-count (.rowCount in-rel)]
        (util/with-close-on-catch [^StructVector struct-vec (-> ^Field (apply types/->field (str col-name) #xt.arrow/type :struct false
                                                                              (for [^IVectorReader col in-rel]
                                                                                (types/field-with-name (.getField col) (.getName col))))
                                                                (.createVector allocator))]

          ;; TODO can we quickly set all of these to 1?
          (dotimes [idx row-count]
            (.setIndexDefined struct-vec idx))

          (doseq [^IVectorReader col in-rel]
            (.copyTo col (.getChild struct-vec (.getName col))))

          (.setValueCount struct-vec (.rowCount in-rel))

          (vr/vec->reader struct-vec))))))

(defrecord RenameProjectionSpec [to-name from-name col-type]
  IProjectionSpec
  (getColumnName [_] to-name)
  (getColumnType [_] col-type)
  (project [_ _allocator in-rel _params]
    (-> (.readerForName in-rel (str from-name))
        (.withName (str to-name)))))

(defn ->rename-projection-spec ^xtdb.operator.IProjectionSpec [to-name from-name field]
  (->RenameProjectionSpec to-name from-name (types/field->col-type field)))

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

(defmethod lp/emit-expr :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [param-fields] :as args}]
  (let [emitted-child-relation (lp/emit-expr relation args)]
    (lp/unary-expr emitted-child-relation
      (fn [inner-fields]
        (let [projection-specs (concat (when append-columns?
                                         (for [[col-name field] inner-fields]
                                           (->identity-projection-spec col-name field)))
                                       (for [[p-type arg] projections]
                                         (case p-type
                                           :column (->identity-projection-spec arg (get inner-fields arg))

                                           :row-number-column (let [[col-name _form] (first arg)]
                                                                (->row-number-projection-spec col-name))

                                           :star (let [[col-name _star] (first arg)]
                                                   (->star-projection-spec col-name [:struct (update-vals inner-fields types/field->col-type)]))

                                           :rename (let [[to-name from-name] (first arg)]
                                                     (->rename-projection-spec to-name from-name (get inner-fields from-name)))

                                           :extend (let [[col-name form] (first arg)
                                                         input-types {:col-types (update-vals inner-fields types/field->col-type)
                                                                      :param-types (update-vals param-fields types/field->col-type)}
                                                         expr (expr/form->expr form input-types)]
                                                     (expr/->expression-projection-spec col-name expr input-types)))))]
          {:fields (->> projection-specs
                        (into {} (map (juxt #(.getColumnName ^IProjectionSpec %)
                                            (comp types/col-type->field #(.getColumnType ^IProjectionSpec %))))))
           :stats (:stats emitted-child-relation)
           :->cursor (fn [opts in-cursor] (->project-cursor opts in-cursor projection-specs))})))))

(defmethod lp/emit-expr :map [op args]
  (lp/emit-expr (assoc op :op :project :opts {:append-columns? true}) args))
