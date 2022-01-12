(ns core2.expression.map
  (:require [clojure.set :as set]
            [core2.expression :as expr]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import [core2.vector IIndirectRelation IIndirectVector IRowCopier IVectorWriter]
           java.lang.AutoCloseable
           [java.util HashMap List Map]
           java.util.function.Function
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.hash.MurmurHasher
           [org.roaringbitmap IntConsumer RoaringBitmap]))

(def ^:private ^org.apache.arrow.memory.util.hash.ArrowBufHasher hasher
  (MurmurHasher.))

(definterface IIndexHasher
  (^int hashCode [^int idx]))

(defn ->hasher ^core2.expression.map.IIndexHasher [^List #_<IIndirectVector> cols]
  (let [hashers (mapv (fn [^IIndirectVector col]
                        (let [v (.getVector col)]
                          (reify IIndexHasher
                            (hashCode [_ idx]
                              (.hashCode v (.getIndex col idx) hasher)))))
                      cols)]
    (reify IIndexHasher
      (hashCode [_ idx]
        (loop [[^IIndexHasher hasher & more-hashers] hashers
               hash-code 0]
          (if hasher
            (recur more-hashers (MurmurHasher/combineHashCode hash-code (.hashCode hasher idx)))
            hash-code))))))

(definterface IRelationMapBuilder
  (^void add [^int inIdx])
  (^int addIfNotPresent [^int inIdx]))

(definterface IRelationMapProber
  (^int indexOf [^int inIdx])
  (^org.roaringbitmap.RoaringBitmap getAll [^int inIdx]))

(definterface IRelationMap
  (^core2.expression.map.IRelationMapBuilder buildFromRelation [^core2.vector.IIndirectRelation inRelation])
  (^core2.expression.map.IRelationMapProber probeFromRelation [^core2.vector.IIndirectRelation inRelation])
  (^core2.vector.IIndirectRelation getBuiltRelation []))

(definterface IntIntPredicate
  (^boolean test [^int l, ^int r]))

(defn- andIIP
  ([]
   (reify IntIntPredicate
     (test [_ _l _r]
       true)))

  ([^IntIntPredicate p1, ^IntIntPredicate p2]
   (reify IntIntPredicate
     (test [_ l r]
       (and (.test p1 l r)
            (.test p2 l r))))))

(defn- ->comparator ^core2.expression.map.IntIntPredicate [left-cols right-cols]
  (->> (map (fn [^IIndirectVector left-col, ^IIndirectVector right-col]
              (let [f (eval
                       (let [left-val-types (expr/field->value-types (.getField (.getVector left-col)))
                             right-val-types (expr/field->value-types (.getField (.getVector right-col)))
                             left-vec (gensym 'left-vec)
                             left-idx (gensym 'left-idx)
                             right-vec (gensym 'right-vec)
                             right-idx (gensym 'right-idx)]
                         `(fn [~(expr/with-tag left-vec IIndirectVector)
                               ~(expr/with-tag right-vec IIndirectVector)]
                            (reify IntIntPredicate
                              (test [_ ~left-idx ~right-idx]
                                ~(let [{continue-left :continue}
                                       (-> (expr/form->expr left-vec {})
                                           (assoc :idx left-idx)
                                           (expr/codegen-expr {:var->types {left-vec left-val-types}}))

                                       {continue-right :continue}
                                       (-> (expr/form->expr right-vec {})
                                           (assoc :idx right-idx)
                                           (expr/codegen-expr {:var->types {right-vec right-val-types}}))]

                                   (continue-left
                                    (fn [left-type left-code]
                                      (continue-right
                                       (fn [right-type right-code]
                                         (let [{continue-= :continue-call}
                                               (expr/codegen-call {:op :call, :f :=,
                                                                   :arg-types [left-type right-type]})]
                                           (continue-= (fn [_out-type out-code] out-code)
                                                       [left-code right-code]))))))))))))]

                (f left-col right-col)))
            left-cols
            right-cols)

       (reduce andIIP)))

(defn- find-in-hash-bitmap ^long [^RoaringBitmap hash-bitmap, ^IntIntPredicate comparator, ^long idx]
  (if-not hash-bitmap
    -1
    (let [it (.getIntIterator hash-bitmap)]
      (loop []
        (if-not (.hasNext it)
          -1
          (let [test-idx (.next it)]
            (if (.test comparator idx test-idx)
              test-idx
              (recur))))))))

(defn returned-idx ^long [^long inserted-idx]
  (-> inserted-idx - dec))

(defn inserted-idx ^long [^long returned-idx]
  (cond-> returned-idx
    (neg? returned-idx) (-> inc -)))

(defn ->relation-map [^BufferAllocator allocator,
                      {:keys [key-col-names build-key-col-names probe-key-col-names store-col-names]
                       :or {build-key-col-names key-col-names
                            probe-key-col-names key-col-names}}]
  (let [hash->bitmap (HashMap.)
        out-rel (vw/->rel-writer allocator)]
    (doseq [col-name build-key-col-names]
      (.writerForName out-rel col-name))

    (letfn [(compute-hash-bitmap [row-hash]
              (.computeIfAbsent hash->bitmap row-hash
                                (reify Function
                                  (apply [_ _]
                                    (RoaringBitmap.)))))]
      (reify
        IRelationMap
        (buildFromRelation [_ in-rel]
          (let [in-rel (if store-col-names
                         (->> (set/union (set build-key-col-names) (set store-col-names))
                              (mapv #(.vectorForName in-rel %))
                              iv/->indirect-rel)
                         in-rel)
                in-key-cols (mapv #(.vectorForName in-rel %) build-key-col-names)
                out-writers (->> (mapv #(.writerForName out-rel (.getName ^IIndirectVector %)) in-rel))
                out-copiers (mapv vw/->row-copier out-writers in-rel)
                build-rel (vw/rel-writer->reader out-rel)

                comparator (->comparator in-key-cols (mapv #(.vectorForName build-rel %) build-key-col-names))
                hasher (->hasher in-key-cols)]

            (letfn [(add ^long [^RoaringBitmap hash-bitmap, ^long idx]
                      (let [out-idx (.getValueCount (.getVector ^IVectorWriter (first out-writers)))]
                        (.add hash-bitmap out-idx)

                        (doseq [^IRowCopier copier out-copiers]
                          (.copyRow copier idx))

                        (returned-idx out-idx)))]

              (reify IRelationMapBuilder
                (add [_ idx]
                  (add (compute-hash-bitmap (.hashCode hasher idx)) idx))

                (addIfNotPresent [_ idx]
                  (let [^RoaringBitmap hash-bitmap (compute-hash-bitmap (.hashCode hasher idx))
                        out-idx (find-in-hash-bitmap hash-bitmap comparator idx)]
                    (if-not (neg? out-idx)
                      out-idx
                      (add hash-bitmap idx))))))))

        (probeFromRelation [_ probe-rel]
          (let [in-key-cols (mapv #(.vectorForName probe-rel %) probe-key-col-names)
                build-rel (vw/rel-writer->reader out-rel)
                comparator (->comparator in-key-cols (mapv #(.vectorForName build-rel %) build-key-col-names))
                hasher (->hasher in-key-cols)]

            (reify IRelationMapProber
              (indexOf [_ idx]
                (-> ^RoaringBitmap (.get hash->bitmap (.hashCode hasher idx))
                    (find-in-hash-bitmap comparator idx)))

              (getAll [_ idx]
                (let [res (RoaringBitmap.)]
                  (some-> ^RoaringBitmap (.get hash->bitmap (.hashCode hasher idx))
                          (.forEach (reify IntConsumer
                                      (accept [_ out-idx]
                                        (when (.test comparator idx out-idx)
                                          (.add res out-idx))))))
                  (when-not (.isEmpty res)
                    res))))))

        (getBuiltRelation [_]
          (vw/rel-writer->reader out-rel))

        AutoCloseable
        (close [_]
          (util/try-close out-rel))))))
