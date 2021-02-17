(ns core2.stream
  (:require [core2.util :as util]
            [core2.types :as t])
  (:import [java.util Spliterator Spliterator$OfDouble Spliterator$OfInt Spliterator$OfLong]
           [java.util.stream StreamSupport Stream IntStream LongStream DoubleStream]
           [java.util.function BiConsumer Consumer DoubleConsumer IntConsumer LongConsumer Supplier ObjDoubleConsumer ObjIntConsumer ObjLongConsumer]
           java.time.LocalDateTime
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.util.Text
           org.apache.arrow.vector.types.pojo.Field
           [org.apache.arrow.vector BigIntVector Float8Vector IntVector NullVector TinyIntVector ValueVector]
           org.apache.arrow.vector.util.VectorBatchAppender))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^{:tag 'long} default-characteristics
  (bit-or Spliterator/ORDERED Spliterator/SIZED Spliterator/SUBSIZED Spliterator/IMMUTABLE Spliterator/NONNULL))

(defprotocol PArrowToClojure
  (arrow->clojure [this]))

(extend-protocol PArrowToClojure
  Text
  (arrow->clojure [this]
    (str this))

  LocalDateTime
  (arrow->clojure [this]
    (util/local-date-time->date this))

  Object
  (arrow->clojure [this]
    this)

  nil
  (arrow->clojure [this]))

(deftype ValueVectorSpliterator [^ValueVector v ^:unsynchronized-mutable ^int idx ^int end-idx ^int characteristics]
  Spliterator
  (^boolean tryAdvance [this ^Consumer f]
   (if (< idx end-idx)
     (do (.accept f (arrow->clojure (.getObject v idx)))
         (set! (.idx this) (inc idx))
         true)
     false))

  (^void forEachRemaining [this ^Consumer f]
   (let [n end-idx]
     (loop [idx idx]
       (if (< idx n)
         (do (.accept f (arrow->clojure (.getObject v idx)))
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

(extend-protocol PSpliterator
  ValueVector
  (->spliterator [this]
    (->ValueVectorSpliterator this 0 (.getValueCount this) default-characteristics))

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

(defprotocol PStreamToVector
  (stream->vector [this field allocator]))

(def ^:private vector-combiner
  (reify BiConsumer
    (accept [_ x y]
      (VectorBatchAppender/batchAppend x (make-array ValueVector [y])))))

(extend-protocol PStreamToVector
  Stream
  (stream->vector [this field allocator]
    (.collect this
              (reify Supplier
                (get [_]
                  (.createVector ^Field field allocator)))
              (reify BiConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (t/set-safe! v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  IntStream
  (stream->vector [this _ allocator]
    (.collect this
              (reify Supplier
                (get [_]
                  (IntVector. "" ^BufferAllocator allocator)))
              (reify ObjIntConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^IntVector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  LongStream
  (stream->vector [this _ allocator]
    (.collect this
              (reify Supplier
                (get [_]
                  (BigIntVector. "" ^BufferAllocator allocator)))
              (reify ObjLongConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^BigIntVector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner))

  DoubleStream
  (stream->vector [this _ allocator]
    (.collect this
              (reify Supplier
                (get [_]
                  (Float8Vector. "" ^BufferAllocator allocator)))
              (reify ObjDoubleConsumer
                (accept [_ v x]
                  (let [c (.getValueCount ^ValueVector v)]
                    (.setSafe ^Float8Vector v c x)
                    (.setValueCount ^ValueVector v (inc c)))))
              vector-combiner)))
