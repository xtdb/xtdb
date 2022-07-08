(ns core2.expression.map
  (:require [core2.expression :as expr]
            [core2.expression.walk :as ewalk]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector :as vec]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           io.netty.util.collection.IntObjectHashMap
           (java.lang AutoCloseable)
           java.util.List
           java.util.function.IntBinaryOperator
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
  (^void forEachMatch [^int inIdx, ^java.util.function.IntConsumer c])
  (^int matches [^int inIdx]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IRelationMap
  (^java.util.Map buildColumnTypes [])
  (^java.util.List buildKeyColumnNames [])
  (^java.util.Map probeColumnTypes [])
  (^java.util.List probeKeyColumnNames [])

  (^core2.expression.map.IRelationMapBuilder buildFromRelation [^core2.vector.IIndirectRelation inRelation])
  (^core2.expression.map.IRelationMapProber probeFromRelation [^core2.vector.IIndirectRelation inRelation])
  (^core2.vector.IIndirectRelation getBuiltRelation []))

(defn- andIBO
  ([]
   (reify IntBinaryOperator
     (applyAsInt [_ _l _r]
       1)))

  ([^IntBinaryOperator p1, ^IntBinaryOperator p2]
   (reify IntBinaryOperator
     (applyAsInt [_ l r]
       (let [l-res (.applyAsInt p1 l r)]
         (if (= -1 l-res)
           -1
           (Math/min l-res (.applyAsInt p2 l r))))))))

(def ^:private left-rel (gensym 'left-rel))
(def ^:private left-vec (gensym 'left-vec))
(def ^:private left-idx (gensym 'left-idx))

(def ^:private right-rel (gensym 'right-rel))
(def ^:private right-vec (gensym 'right-vec))
(def ^:private right-idx (gensym 'right-idx))

(def build-comparator
  (-> (fn [expr input-opts]
        (let [{:keys [continue], :as emitted-expr}
              (expr/codegen-expr expr input-opts)]

          (-> `(fn [~(expr/with-tag left-rel IIndirectRelation)
                    ~(expr/with-tag right-rel IIndirectRelation)
                    ~expr/params-sym]
                 (let [~@(expr/batch-bindings emitted-expr)]
                   (reify IntBinaryOperator
                     (~'applyAsInt [_# ~left-idx ~right-idx]
                      ~(continue (fn [res-type code]
                                   (case res-type
                                     :null 0
                                     :bool `(if ~code 1 -1))))))))

              #_(doto clojure.pprint/pprint)
              (eval))))
      (util/lru-memoize)))

(defn- ->equi-comparator [^IIndirectVector left-col, ^IIndirectVector right-col, params
                          {:keys [nil-keys-equal? param-types]}]
  (let [f (build-comparator {:op :call, :f (if nil-keys-equal? :null-eq :=)
                             :args [{:op :variable, :variable left-vec, :rel left-rel, :idx left-idx}
                                    {:op :variable, :variable right-vec, :rel right-rel, :idx right-idx}]}
                            {:var->col-type {left-vec (types/field->col-type (.getField (.getVector left-col)))
                                             right-vec (types/field->col-type (.getField (.getVector right-col)))}
                             :param-types param-types})]
    (f (iv/->indirect-rel [(.withName left-col (name left-vec))])
       (iv/->indirect-rel [(.withName right-col (name right-vec))])
       params)))

(defn- ->theta-comparator [probe-rel build-rel theta-expr params {:keys [build-col-types probe-col-types param-types]}]
  (let [col-types (merge build-col-types probe-col-types)
        f (build-comparator (->> (expr/form->expr theta-expr {:col-types col-types, :param-types param-types})
                                 (expr/prepare-expr)
                                 (ewalk/postwalk-expr (fn [{:keys [op] :as expr}]
                                                        (cond-> expr
                                                          (= op :variable)
                                                          (into (let [{:keys [variable]} expr]
                                                                  (if (contains? probe-col-types variable)
                                                                    {:rel left-rel, :idx left-idx}
                                                                    {:rel right-rel, :idx right-idx})))))))
                            {:var->col-type col-types, :param-types param-types})]
    (f probe-rel build-rel params)))

(defn- find-in-hash-bitmap ^long [^RoaringBitmap hash-bitmap, ^IntBinaryOperator comparator, ^long idx]
  (if-not hash-bitmap
    -1
    (let [it (.getIntIterator hash-bitmap)]
      (loop []
        (if-not (.hasNext it)
          -1
          (let [test-idx (.next it)]
            (if (= 1 (.applyAsInt comparator idx test-idx))
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
           build-col-types probe-col-types
           with-nil-row? nil-keys-equal?
           theta-expr param-types params]
    :as opts}]
  (let [build-key-col-names (get opts :build-key-col-names key-col-names)
        probe-key-col-names (get opts :probe-key-col-names key-col-names)

        hash->bitmap (IntObjectHashMap.)
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

                  in-key-cols (mapv #(.vectorForName in-rel (name %))
                                    build-key-col-names)

                  ;; NOTE: we might not need to compute `comparator` if the caller never requires `addIfNotPresent` (e.g. joins)
                  !comparator (delay
                                (->> (map (fn [build-col in-col]
                                            (->equi-comparator in-col build-col params
                                                               {:nil-keys-equal? nil-keys-equal?,
                                                                :param-types param-types}))
                                          build-key-cols
                                          in-key-cols)
                                     (reduce andIBO)))

                  hasher (->hasher in-key-cols)

                  row-copier (.rowCopier rel-writer in-rel)]

              (letfn [(add ^long [^RoaringBitmap hash-bitmap, ^long idx]
                        (let [out-idx (.copyRow row-copier idx)]
                          (.add hash-bitmap out-idx)
                          (returned-idx out-idx)))]

                (reify IRelationMapBuilder
                  (add [_ idx]
                    (add (compute-hash-bitmap (.hashCode hasher idx)) idx))

                  (addIfNotPresent [_ idx]
                    (let [^RoaringBitmap hash-bitmap (compute-hash-bitmap (.hashCode hasher idx))
                          out-idx (find-in-hash-bitmap hash-bitmap @!comparator idx)]
                      (if-not (neg? out-idx)
                        out-idx
                        (add hash-bitmap idx))))))))

          (probeFromRelation [this probe-rel]
            (let [build-rel (.getBuiltRelation this)
                  probe-key-cols (mapv #(.vectorForName probe-rel (name %))
                                       probe-key-col-names)

                  ^IntBinaryOperator
                  comparator (->> (cond-> (map (fn [build-col probe-col]
                                                 (->equi-comparator probe-col build-col params
                                                                    {:nil-keys-equal? nil-keys-equal?
                                                                     :left-col-types probe-col-types
                                                                     :right-col-types build-col-types
                                                                     :param-types param-types}))
                                               build-key-cols
                                               probe-key-cols)

                                    (some? theta-expr)
                                    (conj (->theta-comparator probe-rel build-rel theta-expr params
                                                              {:build-col-types build-col-types
                                                               :probe-col-types probe-col-types
                                                               :param-types param-types})))
                                 (reduce andIBO))

                  hasher (->hasher probe-key-cols)]

              (reify IRelationMapProber
                (indexOf [_ idx]
                  (-> ^RoaringBitmap (.get hash->bitmap (.hashCode hasher idx))
                      (find-in-hash-bitmap comparator idx)))

                (forEachMatch [_ idx c]
                  (some-> ^RoaringBitmap (.get hash->bitmap (.hashCode hasher idx))
                          (.forEach (reify IntConsumer
                                      (accept [_ out-idx]
                                        (when (= 1 (.applyAsInt comparator idx out-idx))
                                          (.accept c out-idx)))))))


                (matches [_ probe-idx]
                  ;; TODO: this doesn't use the hashmaps, still a nested loop join
                  (let [acc (int-array [-1])]
                    (loop [build-idx 0]
                      (if (= build-idx (.rowCount build-rel))
                        (aget acc 0)
                        (let [res (.applyAsInt comparator probe-idx build-idx)]
                          (if (= 1 res)
                            1
                            (do
                              (aset acc 0 (Math/max (aget acc 0) res))
                              (recur (inc build-idx))))))))))))

          (getBuiltRelation [_]
            (let [pos (.getPosition (.writerPosition rel-writer))]
              (doseq [^IVectorWriter w rel-writer]
                (.setValueCount (.getVector w) pos)))

            (iv/->indirect-rel (mapv #(iv/->direct-vec (.getVector ^IVectorWriter %)) rel-writer)))

          AutoCloseable
          (close [_] (.close rel-writer)))))))
