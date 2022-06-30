(ns core2.expression.map
  (:require [core2.expression :as expr]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector :as vec]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2.vector IIndirectVector IVectorWriter)
           io.netty.util.collection.IntObjectHashMap
           (java.lang AutoCloseable)
           (java.util HashMap List)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.memory.util.hash MurmurHasher)
           (org.apache.arrow.vector NullVector)
           (org.roaringbitmap IntConsumer RoaringBitmap)))

(def ^:private ^org.apache.arrow.memory.util.hash.ArrowBufHasher hasher
  (MurmurHasher.))

(definterface IIndexHasher
  (^int hashCode [^int idx]))

(defn ->hasher ^core2.expression.map.IIndexHasher [^List #_<IIndirectVector> cols]
  (case (.size cols)
    1 (let [^IIndirectVector col (.get cols 0)
            v (.getVector col)]
        (reify IIndexHasher
          (hashCode [_ idx]
            (.hashCode v (.getIndex col idx) hasher))))

    (reify IIndexHasher
      (hashCode [_ idx]
        (loop [n 0
               hash-code 0]
          (if (< n (.size cols))
            (let [^IIndirectVector col (.get cols n)
                  v (.getVector col)]
              (recur (inc n) (MurmurHasher/combineHashCode hash-code (.hashCode v (.getIndex col idx) hasher))))
            hash-code))))))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IRelationMapBuilder
  (^void add [^int inIdx])
  (^int addIfNotPresent [^int inIdx]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IRelationMapProber
  (^int indexOf [^int inIdx])
  (^org.roaringbitmap.RoaringBitmap getAll [^int inIdx]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IRelationMap
  (^java.util.Map buildColumnTypes [])
  (^java.util.List buildKeyColumnNames [])
  (^java.util.Map probeColumnTypes [])
  (^java.util.List probeKeyColumnNames [])

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

(def build-comparator
  (-> (fn [left-col-type right-col-type nil-equal]
        (let [left-vec (gensym 'left-vec)
              left-idx (gensym 'left-idx)
              right-vec (gensym 'right-vec)
              right-idx (gensym 'right-idx)
              eq-fn (if nil-equal :null-eq :=)

              return-boxes (HashMap.)

              {:keys [continue], :as emitted-expr}
              (expr/codegen-expr {:op :call, :f :boolean
                                  :args [{:op :call, :f eq-fn
                                          :args [{:op :variable, :variable left-vec, :idx left-idx}
                                                 {:op :variable, :variable right-vec, :idx right-idx}]}]}
                                 {:var->col-type {left-vec left-col-type
                                                  right-vec right-col-type}
                                  :return-boxes return-boxes
                                  :extract-vecs-from-rel? false})]

          (-> `(fn [~(expr/with-tag left-vec IIndirectVector)
                    ~(expr/with-tag right-vec IIndirectVector)]
                 (let [~@(expr/batch-bindings emitted-expr)]
                   (reify IntIntPredicate
                     (~'test [_# ~left-idx ~right-idx]
                      ~(continue (fn [_ code] code))))))

              #_(doto clojure.pprint/pprint)
              (eval))))
      (util/lru-memoize)))

(defn- ->comparator ^core2.expression.map.IntIntPredicate [left-cols right-cols nil-equal]
  (->> (map (fn [^IIndirectVector left-col, ^IIndirectVector right-col]
              (let [left-col-type (types/field->col-type (.getField (.getVector left-col)))
                    right-col-type (types/field->col-type (.getField (.getVector right-col)))
                    f (build-comparator left-col-type right-col-type nil-equal)]
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

(defn ->nil-rel
  "Returns a single row relation where all columns are nil. (Useful for outer joins)."
  [col-names]
  (iv/->indirect-rel (for [col-name col-names]
                       (iv/->direct-vec (doto (NullVector. (name col-name))
                                          (.setValueCount 1))))))

(def nil-row-idx 0)

(defn ->relation-map ^core2.expression.map.IRelationMap
  [^BufferAllocator allocator,
   {:keys [key-col-names store-full-build-rel?
           build-col-types build-key-col-names
           probe-col-types probe-key-col-names
           with-nil-row? nil-keys-equal?]
    :or {build-key-col-names key-col-names
         probe-key-col-names key-col-names}}]

  (let [hash->bitmap (IntObjectHashMap.)
        rel-writer (vw/->rel-writer allocator)]

    (doseq [[col-name col-type] (cond-> build-col-types
                                  (not store-full-build-rel?) (select-keys build-key-col-names)

                                  with-nil-row? (->> (into {} (map (juxt key
                                                                         (comp (fn [col-type]
                                                                                 (cond-> col-type
                                                                                   with-nil-row? (types/merge-col-types :null)))
                                                                               val))))))]
      (.writerForName rel-writer (name col-name) col-type))

    (when with-nil-row?
      (doto (.rowCopier rel-writer (->nil-rel (keys build-col-types)))
        (.copyRow 0)))

    (let [build-key-cols (mapv #(iv/->direct-vec (.getVector (.writerForName rel-writer (name %))))
                               build-key-col-names)]
      (letfn [(compute-hash-bitmap [^long row-hash]
                (or (.get hash->bitmap row-hash)
                    (let [bitmap (RoaringBitmap.)]
                      (.put hash->bitmap (int row-hash) bitmap)
                      bitmap)))]
        (reify
          IRelationMap
          (buildColumnTypes [_] build-col-types)
          (buildKeyColumnNames [_] build-key-col-names)
          (probeColumnTypes [_] probe-col-types)
          (probeKeyColumnNames [_] probe-key-col-names)

          (buildFromRelation [_ in-rel]
            (let [in-rel (if store-full-build-rel?
                           in-rel
                           (->> (set build-key-col-names)
                                (mapv #(.vectorForName in-rel (name %)))
                                iv/->indirect-rel))
                  in-key-cols (mapv #(.vectorForName in-rel (name %)) build-key-col-names)
                  row-copier (.rowCopier rel-writer in-rel)
                  comparator (->comparator in-key-cols build-key-cols nil-keys-equal?)
                  hasher (->hasher in-key-cols)]

              (letfn [(add ^long [^RoaringBitmap hash-bitmap, ^long idx]
                        (let [out-idx (.copyRow row-copier idx)]
                          (.add hash-bitmap out-idx)
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
            (let [in-key-cols (mapv #(.vectorForName probe-rel (name %)) probe-key-col-names)
                  comparator (->comparator in-key-cols build-key-cols nil-keys-equal?)
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
            (let [pos (.getPosition (.writerPosition rel-writer))]
              (doseq [^IVectorWriter w rel-writer]
                (.setValueCount (.getVector w) pos)))

            (iv/->indirect-rel (mapv #(iv/->direct-vec (.getVector ^IVectorWriter %)) rel-writer)))

          AutoCloseable
          (close [_] (.close rel-writer)))))))
