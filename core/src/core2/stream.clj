(ns core2.stream
  (:require [core2.util :as util]
            [core2.types :as t])
  (:import [java.util Spliterator Spliterator$OfDouble Spliterator$OfInt Spliterator$OfLong]
           [java.util.stream StreamSupport Stream IntStream LongStream DoubleStream]
           [java.util.function BiConsumer Consumer DoubleConsumer IntConsumer LongConsumer Supplier ObjDoubleConsumer ObjIntConsumer ObjLongConsumer]
           java.time.LocalDateTime
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Field
           [org.apache.arrow.vector BigIntVector Float8Vector IntVector NullVector TinyIntVector ValueVector]
           [org.apache.arrow.vector.complex DenseUnionVector VectorWithOrdinal]
           org.apache.arrow.vector.complex.reader.FieldReader
           org.apache.arrow.vector.util.VectorBatchAppender))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^{:tag 'long} default-characteristics
  (bit-or Spliterator/ORDERED Spliterator/SIZED Spliterator/SUBSIZED Spliterator/IMMUTABLE Spliterator/NONNULL))

(deftype ValueVectorWithOrdinalSpliterator [^ValueVector v ^:unsynchronized-mutable ^int idx ^int end-idx ^int characteristics]
  Spliterator
  (^boolean tryAdvance [this ^Consumer f]
   (if (< idx end-idx)
     (do (.accept f (VectorWithOrdinal. v idx))
         (set! (.idx this) (inc idx))
         true)
     false))

  (^void forEachRemaining [this ^Consumer f]
   (let [n end-idx]
     (loop [idx idx]
       (if (< idx n)
         (do (.accept f (VectorWithOrdinal. v idx))
             (recur (inc idx)))
         (set! (.idx this) idx)))))

  (trySplit [this]
    (let [idx idx
          mid (quot (- end-idx idx) 2)]
      (when (< idx mid)
        (set! (.idx this) mid)
        (ValueVectorWithOrdinalSpliterator. v idx mid characteristics))))

  (estimateSize [_]
    (- end-idx idx))

  (characteristics [_]
    characteristics))


(deftype FieldReaderSpliterator [^ValueVector v ^FieldReader r ^int end-idx ^int characteristics]
  Spliterator
  (^boolean tryAdvance [this ^Consumer f]
   (if (< (.getPosition r) end-idx)
     (do (.accept f r)
         (.setPosition r (inc (.getPosition r)))
         true)
     false))

  (^void forEachRemaining [this ^Consumer f]
   (let [n end-idx]
     (loop [idx (.getPosition r)]
       (doto r
         (.setPosition idx))
       (when (< idx n)
         (do (.accept f r)
             (recur (inc idx)))))))

  (trySplit [this]
    (let [idx (.getPosition r)
          mid (quot (- end-idx idx) 2)]
      (when (< idx mid)
        (.setPosition r mid)
        (FieldReaderSpliterator. v
                                 (doto (.getReader v)
                                   (.setPosition idx))
                                 mid
                                 characteristics))))

  (estimateSize [_]
    (- end-idx (.getPosition r)))

  (characteristics [_]
    characteristics))

(deftype ValueVectorSpliterator [^ValueVector v ^:unsynchronized-mutable ^int idx ^int end-idx ^int characteristics]
  Spliterator
  (^boolean tryAdvance [this ^Consumer f]
   (if (< idx end-idx)
     (do (.accept f (t/get-object v idx))
         (set! (.idx this) (inc idx))
         true)
     false))

  (^void forEachRemaining [this ^Consumer f]
   (let [n end-idx]
     (loop [idx idx]
       (if (< idx n)
         (do (.accept f (t/get-object v idx))
             (recur (inc idx)))
         (set! (.idx this) idx)))))

  (trySplit [this]
    (let [idx idx
          mid (quot (- end-idx idx) 2)]
      (when (< idx mid)
        (set! (.idx this) mid)
        (ValueVectorSpliterator. v idx mid characteristics))))

  (estimateSize [_]
    (- end-idx idx))

  (characteristics [_]
    characteristics))

(defmacro ^:private def-spliterator [t st ct]
  (let [vec-sym (gensym 'vec)
        idx-sym (gensym 'idx)
        end-idx-sym (gensym 'end-idx)
        f-sym (gensym 'f)]
    `(deftype ~(symbol (str t "Spliterator"))
         [~(with-meta vec-sym {:tag t})
          ~(with-meta idx-sym {:unsynchronized-mutable true :tag 'int})
          ~(with-meta end-idx-sym {:tag 'int})]
       ~st
       (^boolean tryAdvance [this# ~(with-meta f-sym {:tag ct})]
        (if (< ~idx-sym ~end-idx-sym)
          (do (.accept ~f-sym (.get ~vec-sym ~idx-sym))
              (set! (~(symbol (str "." idx-sym)) this#) (inc ~idx-sym))
              true)
          false))

       (^void forEachRemaining [this# ~(with-meta f-sym {:tag ct})]
        (let [n# ~end-idx-sym]
          (loop [idx# ~idx-sym]
            (if (< idx# n#)
              (do (.accept ~f-sym (.get ~vec-sym idx#))
                  (recur (inc idx#)))
              (set! (~(symbol (str "." idx-sym)) this#) idx#)))))

       (trySplit [this#]
         (let [idx# ~idx-sym
               mid# (quot (- ~end-idx-sym idx#) 2)]
           (when (< idx# mid#)
             (set! (~(symbol (str "." idx-sym)) this#) mid#)
             (~(symbol (str t "Spliterator.")) ~vec-sym idx# mid#))))

       (estimateSize [_]
         (- ~end-idx-sym ~idx-sym))

       (characteristics [_]
         ~default-characteristics))))

(def-spliterator BigIntVector Spliterator$OfLong LongConsumer)
(def-spliterator IntVector Spliterator$OfInt IntConsumer)
(def-spliterator TinyIntVector Spliterator$OfInt IntConsumer)
(def-spliterator Float8Vector Spliterator$OfDouble DoubleConsumer)

(defprotocol PSpliterator
  (->spliterator [this]))

(defn- maybe-single-child-dense-union ^org.apache.arrow.vector.ValueVector [^ValueVector v]
  (or (when (instance? DenseUnionVector v)
        (let [children-with-elements (for [^ValueVector child (.getChildrenFromFields ^DenseUnionVector v)
                                           :when (pos? (.getValueCount child))]
                                       child)]
          (when (= 1 (count children-with-elements))
            (first children-with-elements))))
      v))

(extend-protocol PSpliterator
  ValueVector
  (->spliterator [this]
    (->ValueVectorSpliterator this 0 (.getValueCount this) default-characteristics))

  DenseUnionVector
  (->spliterator [this]
    (let [v (maybe-single-child-dense-union this)]
      (if (instance? DenseUnionVector v)
        (->ValueVectorSpliterator this 0 (.getValueCount this) default-characteristics)
        (->spliterator v))))

  NullVector
  (->spliterator [this]
    (->ValueVectorSpliterator this 0 (.getValueCount this) (bit-and-not default-characteristics Spliterator/NONNULL)))

  TinyIntVector
  (->spliterator [this]
    (->TinyIntVectorSpliterator this 0 (.getValueCount this)))

  IntVector
  (->spliterator [this]
    (->IntVectorSpliterator this 0 (.getValueCount this)))

  BigIntVector
  (->spliterator [this]
    (->BigIntVectorSpliterator this 0 (.getValueCount this)))

  Float8Vector
  (->spliterator [this]
    (->Float8VectorSpliterator this 0 (.getValueCount this)))

  Spliterator
  (->spliterator [this] this))

(defn ->spliterator-with-ordinal ^java.util.Spliterator [^ValueVector v]
  (->ValueVectorWithOrdinalSpliterator v 0 (.getValueCount v) default-characteristics))

(defn ->spliterator-with-field-reader ^java.util.Spliterator [^ValueVector v]
  (->FieldReaderSpliterator v (.getReader v) (.getValueCount v) default-characteristics))

(defn ->stream ^java.util.stream.BaseStream [pspliterator]
  (let [spliterator (->spliterator pspliterator)]
    (cond
      (instance? Spliterator$OfInt spliterator)
      (StreamSupport/intStream spliterator false)

      (instance? Spliterator$OfDouble spliterator)
      (StreamSupport/doubleStream spliterator false)

      (instance? Spliterator$OfLong spliterator)
      (StreamSupport/longStream spliterator false)

      :else
      (StreamSupport/stream spliterator false))))

(defprotocol PStreamToArrow
  (stream->arrow [this to-vector]))

(def ^:private vector-combiner
  (reify BiConsumer
    (accept [_ x y]
      (VectorBatchAppender/batchAppend x (make-array ValueVector [y])))))

(extend-protocol PStreamToArrow
  Stream
  (stream->arrow [this to-vector]
    (.collect this
              (util/->supplier (constantly to-vector))
              (reify BiConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (t/set-safe! v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  IntStream
  (stream->arrow [this to-vector]
    (.collect this
              (util/->supplier (constantly to-vector))
              (reify ObjIntConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^IntVector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  LongStream
  (stream->arrow [this to-vector]
    (.collect this
              (util/->supplier (constantly to-vector))
              (reify ObjLongConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^BigIntVector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  DoubleStream
  (stream->arrow [this to-vector]
    (.collect this
              (util/->supplier (constantly to-vector))
              (reify ObjDoubleConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^Float8Vector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner)))
