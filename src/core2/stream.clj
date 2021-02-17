(ns core2.stream
  (:require [core2.util :as util])
  (:import [java.util Spliterator Spliterator$OfDouble Spliterator$OfInt Spliterator$OfLong]
           java.util.stream.StreamSupport
           [java.util.function Consumer DoubleConsumer IntConsumer LongConsumer]
           java.time.LocalDateTime
           org.apache.arrow.vector.util.Text
           [org.apache.arrow.vector BigIntVector Float8Vector IntVector TimeStampMilliVector TinyIntVector ValueVector]))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^{:tag long} default-characteristics
  (bit-or Spliterator/ORDERED Spliterator/SIZED Spliterator/SUBSIZED Spliterator/NONNULL Spliterator/IMMUTABLE))

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

(deftype ValueVectorSpliterator [^ValueVector v ^:unsynchronized-mutable ^int idx ^int end-idx]
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
        (ValueVectorSpliterator. v idx mid))))

  (estimateSize [_]
    (- end-idx idx))

  (characteristics [_]
    default-characteristics))

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
    (->ValueVectorSpliterator this 0 (.getValueCount this)))

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

  TimeStampMilliVector
  (->spliterator [this]
    (->ValueVectorSpliterator this 0 (.getValueCount this)))

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
