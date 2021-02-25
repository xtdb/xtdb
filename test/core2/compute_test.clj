(ns core2.compute-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector BaseIntVector BaseVariableWidthVector BigIntVector BitVector ElementAddressableVector
            FloatingPointVector Float8Vector TimeStampVector TimeStampMilliVector VarBinaryVector VarCharVector ValueVector]
           org.apache.arrow.vector.util.Text
           [java.util.function DoublePredicate LongPredicate Predicate DoubleUnaryOperator LongUnaryOperator Function]
           [java.util Arrays Date]
           clojure.lang.MapEntry))

;; Arrow compute kernels spike, loosely based on
;; https://arrow.apache.org/docs/cpp/compute.html

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic ^BufferAllocator *allocator*)

(defn maybe-primitive-type-sym [c]
  (get '{Double double Long long} c c))

(defn maybe-array-type-form [c]
  (case c
    bytes `(Class/forName "[B")
    c))

(defmacro def-binop [name & op-signatures]
  `(do
     (defmulti ~(with-meta name {:tag `ValueVector}) (fn [left# right#] (MapEntry/create (type left#) (type right#))))

     ~@(for [[left-type right-type out-type expression inits] op-signatures]
         (let [left-sym (with-meta 'left {:tag (maybe-primitive-type-sym left-type)})
               right-sym (with-meta 'right {:tag (maybe-primitive-type-sym right-type)})
               idx-sym 'idx]
           `(defmethod ~name [~(maybe-array-type-form left-type) ~(maybe-array-type-form right-type)] [~left-sym ~right-sym]
              (let [out# (new ~out-type "" *allocator*)
                    value-count# (.getValueCount ~left-sym)
                    ~@inits]
                (.allocateNew out# value-count#)
                (dotimes [~idx-sym value-count#]
                  (.set out# ~idx-sym ~expression))
                (.setValueCount out# value-count#)
                out#))))))

(defmacro def-unop [name & op-signatures]
  `(do
     (defmulti ~(with-meta name {:tag `ValueVector}) (fn [in#] (type in#)))

     ~@(for [[in-type out-type expression] op-signatures]
         (let [in-sym (with-meta 'in {:tag (->primitive-type-sym in-type)})
               idx-sym 'idx]
           `(defmethod ~name ~in-type [~in-sym]
              (let [out# (new ~out-type "" *allocator*)
                    value-count# (.getValueCount ~in-sym)]
                (.allocateNew out# value-count#)
                (dotimes [~idx-sym value-count#]
                  (.set out# ~idx-sym ~expression))
                (.setValueCount out# value-count#)
                out#))))))

(def-binop add-op
  [BaseIntVector Double Float8Vector
   (+ (.getValueAsLong left idx) right)]
  [BaseIntVector Long BigIntVector
   (+ (.getValueAsLong left idx) right)]
  [BaseIntVector BaseIntVector BigIntVector
   (+ (.getValueAsLong left idx)
      (.getValueAsLong right idx))]
  [BaseIntVector FloatingPointVector Float8Vector
   (+ (.getValueAsLong left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector Double Float8Vector
   (+ (.getValueAsDouble left idx) right)]
  [FloatingPointVector Long Float8Vector
   (+ (.getValueAsDouble left idx) right)]
  [FloatingPointVector FloatingPointVector Float8Vector
   (+ (.getValueAsDouble left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector BaseIntVector Float8Vector
   (+ (.getValueAsDouble left idx)
      (.getValueAsLong right idx))])

(def-binop sub-op
  [BaseIntVector Double Float8Vector
   (- (.getValueAsLong left idx) right)]
  [BaseIntVector Long BigIntVector
   (- (.getValueAsLong left idx) right)]
  [BaseIntVector BaseIntVector BigIntVector
   (- (.getValueAsLong left idx)
      (.getValueAsLong right idx))]
  [BaseIntVector FloatingPointVector Float8Vector
   (- (.getValueAsLong left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector Double Float8Vector
   (- (.getValueAsDouble left idx) right)]
  [FloatingPointVector Long Float8Vector
   (- (.getValueAsDouble left idx) right)]
  [FloatingPointVector FloatingPointVector Float8Vector
   (- (.getValueAsDouble left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector BaseIntVector Float8Vector
   (- (.getValueAsDouble left idx)
      (.getValueAsLong right idx))])

(def-binop mul-op
  [BaseIntVector Double Float8Vector
   (* (.getValueAsLong left idx) right)]
  [BaseIntVector Long BigIntVector
   (* (.getValueAsLong left idx) right)]
  [BaseIntVector BaseIntVector BigIntVector
   (* (.getValueAsLong left idx)
      (.getValueAsLong right idx))]
  [BaseIntVector FloatingPointVector Float8Vector
   (* (.getValueAsLong left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector Double Float8Vector
   (* (.getValueAsDouble left idx) right)]
  [FloatingPointVector Long Float8Vector
   (* (.getValueAsDouble left idx) right)]
  [FloatingPointVector FloatingPointVector Float8Vector
   (* (.getValueAsDouble left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector BaseIntVector Float8Vector
   (* (.getValueAsDouble left idx)
      (.getValueAsLong right idx))])

(def-binop div-op
  [BaseIntVector Double Float8Vector
   (/ (.getValueAsLong left idx) right)]
  [BaseIntVector Long BigIntVector
   (quot (.getValueAsLong left idx) right)]
  [BaseIntVector BaseIntVector BigIntVector
   (quot (.getValueAsLong left idx)
         (.getValueAsLong right idx))]
  [BaseIntVector FloatingPointVector Float8Vector
   (/ (.getValueAsLong left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector Double Float8Vector
   (/ (.getValueAsDouble left idx) right)]
  [FloatingPointVector Long Float8Vector
   (/ (.getValueAsDouble left idx) right)]
  [FloatingPointVector FloatingPointVector Float8Vector
   (/ (.getValueAsDouble left idx)
      (.getValueAsDouble right idx))]
  [FloatingPointVector BaseIntVector Float8Vector
   (/ (.getValueAsDouble left idx)
      (.getValueAsLong right idx))])

(def-unop neg-op
  [BaseIntVector BigIntVector
   (- (.getValueAsLong in idx))]
  [FloatingPointVector Float8Vector
   (- (.getValueAsDouble in idx))])

(defmacro boolean->bit [b]
  `(if ~b 1 0))

(def-binop eq-op
  [BaseIntVector Double BitVector
   (boolean->bit (== (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (= (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (== (.getValueAsLong left idx)
                     (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (= (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (== (.getValueAsDouble left idx) right))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (= (.getDataPointer left idx left-pointer)
                    (.getDataPointer right idx right-pointer)))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (= (.getDataPointer left idx left-pointer) right))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (= (str (.getObject left idx)) right))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (Arrays/equals (.get left idx) right))]
  [BitVector BitVector BitVector
   (boolean->bit (= (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (= (.get left idx) (boolean->bit right)))])

(def-binop neq-op
  [BaseIntVector Double BitVector
   (boolean->bit (not= (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (not= (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (not= (.getValueAsLong left idx)
                       (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (not= (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (not= (.getValueAsDouble left idx) right))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (not= (.getDataPointer left idx left-pointer)
                       (.getDataPointer right idx right-pointer)))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (not= (.getDataPointer left idx left-pointer) right))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (not= (str (.getObject left idx)) right))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (not (Arrays/equals (.get left idx) right)))]
  [BitVector BitVector BitVector
   (boolean->bit (not= (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (not= (.get left idx) (boolean->bit right)))])

(def-binop lt-op
  [BaseIntVector Double BitVector
   (boolean->bit (< (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (< (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (< (.getValueAsLong left idx)
                    (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (< (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (< (.getValueAsDouble left idx) right))]
  [TimeStampVector Long BitVector
   (boolean->bit (< (.get left idx) right))]
  [TimeStampVector Date BitVector
   (boolean->bit (< (.get left idx) (.getTime right)))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (neg? (.compareTo (.getDataPointer left idx left-pointer)
                                   (.getDataPointer right idx right-pointer))))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (neg? (.compareTo (.getDataPointer left idx left-pointer) right)))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (neg? (.compareTo (str (.getObject left idx)) right)))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (neg? (Arrays/compareUnsigned (.get left idx) right)))]
  [BitVector BitVector BitVector
   (boolean->bit (< (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (< (.get left idx) (boolean->bit right)))])

(def-binop le-op
  [BaseIntVector Double BitVector
   (boolean->bit (<= (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (<= (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (<= (.getValueAsLong left idx)
                    (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (<= (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (<= (.getValueAsDouble left idx) right))]
  [TimeStampVector Long BitVector
   (boolean->bit (<= (.get left idx) right))]
  [TimeStampVector Date BitVector
   (boolean->bit (<= (.get left idx) (.getTime right)))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (not (pos? (.compareTo (.getDataPointer left idx left-pointer)
                                        (.getDataPointer right idx right-pointer)))))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (not (pos? (.compareTo (.getDataPointer left idx left-pointer) right))))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (not (pos? (.compareTo (str (.getObject left idx)) right))))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (not (pos? (Arrays/compareUnsigned (.get left idx) right))))]
  [BitVector BitVector BitVector
   (boolean->bit (<= (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (<= (.get left idx) (boolean->bit right)))])

(def-binop gt-op
  [BaseIntVector Double BitVector
   (boolean->bit (> (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (> (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (> (.getValueAsLong left idx)
                    (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (> (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (> (.getValueAsDouble left idx) right))]
  [TimeStampVector Long BitVector
   (boolean->bit (> (.get left idx) right))]
  [TimeStampVector Date BitVector
   (boolean->bit (> (.get left idx) (.getTime right)))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (pos? (.compareTo (.getDataPointer left idx left-pointer)
                                   (.getDataPointer right idx right-pointer))))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (pos? (.compareTo (.getDataPointer left idx left-pointer) right)))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (pos? (.compareTo (str (.getObject left idx)) right)))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (pos? (Arrays/compareUnsigned (.get left idx) right)))]
  [BitVector BitVector BitVector
   (boolean->bit (> (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (> (.get left idx) (boolean->bit right)))])

(def-binop ge-op
  [BaseIntVector Double BitVector
   (boolean->bit (>= (.getValueAsLong left idx) right))]
  [BaseIntVector Long BitVector
   (boolean->bit (>= (.getValueAsLong left idx) right))]
  [BaseIntVector FloatingPointVector BitVector
   (boolean->bit (>= (.getValueAsLong left idx)
                    (.getValueAsDouble right idx)))]
  [FloatingPointVector Double BitVector
   (boolean->bit (>= (.getValueAsDouble left idx) right))]
  [FloatingPointVector Long BitVector
   (boolean->bit (>= (.getValueAsDouble left idx) right))]
  [TimeStampVector Long BitVector
   (boolean->bit (>= (.get left idx) right))]
  [TimeStampVector Date BitVector
   (boolean->bit (>= (.get left idx) (.getTime right)))]
  [ElementAddressableVector ElementAddressableVector BitVector
   (boolean->bit (not (neg? (.compareTo (.getDataPointer left idx left-pointer)
                                        (.getDataPointer right idx right-pointer)))))
   [left-pointer (ArrowBufPointer.)
    right-pointer (ArrowBufPointer.)]]
  [ElementAddressableVector ArrowBufPointer BitVector
   (boolean->bit (not (neg? (.compareTo (.getDataPointer left idx left-pointer) right))))
   [left-pointer (ArrowBufPointer.)]]
  [VarCharVector String BitVector
   (boolean->bit (not (neg? (.compareTo (str (.getObject left idx)) right))))]
  [VarBinaryVector bytes BitVector
   (boolean->bit (not (neg? (Arrays/compareUnsigned (.get left idx) right))))]
  [BitVector BitVector BitVector
   (boolean->bit (>= (.get left idx) (.get right idx)))]
  [BitVector Boolean BitVector
   (boolean->bit (>= (.get left idx) (boolean->bit right)))])

(def-unop not-op
  [BitVector BitVector
   (if (= 1 (.get in idx)) 0 1)])

(def-binop and-op
  [BitVector BitVector BitVector
   (boolean->bit (= 1 (.get left idx) (.get right idx)))])

(def-binop or-op
  [BitVector BitVector BitVector
   (boolean->bit (or (= 1 (.get left idx)) (= 1 (.get right idx))))])

(def-binop udf-op
  [BaseIntVector LongPredicate BitVector
   (boolean->bit (.test right (.getValueAsLong left idx)))]
  [BaseIntVector DoublePredicate BitVector
   (boolean->bit (.test right (.getValueAsLong left idx)))]
  [FloatingPointVector LongPredicate BitVector
   (boolean->bit (.test right (.getValueAsDouble left idx)))]
  [FloatingPointVector DoublePredicate BitVector
   (boolean->bit (.test right (.getValueAsDouble left idx)))]
  [ValueVector Predicate BitVector
   (boolean->bit (.test right (.getObject left idx)))]
  [BaseIntVector LongUnaryOperator BigIntVector
   (.applyAsLong right (.getValueAsLong left idx))]
  [BaseIntVector DoubleUnaryOperator Float8Vector
   (.applyAsDouble right (.getValueAsLong left idx))]
  [Float8Vector LongUnaryOperator BigIntVector
   (.applyAsLong right (.getValueAsDouble left idx))]
  [Float8Vector DoubleUnaryOperator Float8Vector
   (.applyAsDouble right (.getValueAsDouble left idx))]
  [BitVector Function BitVector
   (boolean->bit (.apply right (= 1 (.get left idx))))]
  [TimeStampVector LongUnaryOperator TimeStampMilliVector
   (.applyAsLong right (.get left idx))]
  [TimeStampVector Function TimeStampMilliVector
   (.getTime ^Date (.apply right (Date. (.get left idx))))]
  [VarCharVector Function VarCharVector
   (Text. (str (.apply right (str (.get left idx)))))]
  [VarBinaryVector Function VarBinaryVector
   ^bytes (.apply right (.get left idx))])

(t/deftest can-compute-vectors
  (with-open [a (RootAllocator.)]
    (binding [*allocator* a]
      (with-open [is (doto (BigIntVector. "" *allocator*)
                       (.allocateNew)
                       (.setSafe 0 1)
                       (.setSafe 1 2)
                       (.setSafe 2 3)
                       (.setValueCount 3))
                  fs (doto (Float8Vector. "" *allocator*)
                       (.allocateNew)
                       (.setSafe 0 1.0)
                       (.setSafe 1 2.0)
                       (.setSafe 2 3.0)
                       (.setValueCount 3))
                  is+f (add-op is 2.0)
                  is+i (add-op is 2)
                  is+fs (add-op is fs)
                  is+is (add-op is is)
                  fs+i (add-op fs 2)
                  fs+f (add-op fs 2.0)
                  fs+is (add-op fs is)
                  fs+fs (add-op fs fs)]

        (t/is (= [3.0 4.0 5.0] (tu/->list is+f)))
        (t/is (= [3 4 5] (tu/->list is+i)))
        (t/is (= [2.0 4.0 6.0] (tu/->list is+fs)))
        (t/is (= [2 4 6] (tu/->list is+is)))

        (t/is (= [3.0 4.0 5.0] (tu/->list fs+i)))
        (t/is (= [3.0 4.0 5.0] (tu/->list fs+f)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+is)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+fs)))))))
