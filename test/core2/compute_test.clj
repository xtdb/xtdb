(ns core2.compute-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector BaseIntVector BaseVariableWidthVector BigIntVector BitVector ElementAddressableVector
            FloatingPointVector Float8Vector TimeStampVector TimeStampMilliVector VarBinaryVector VarCharVector ValueVector]
           org.apache.arrow.vector.util.Text
           [java.util.function DoublePredicate LongPredicate Predicate
            DoubleUnaryOperator LongUnaryOperator LongToDoubleFunction DoubleToLongFunction Function
            ToDoubleFunction ToLongFunction]
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

(defmulti ^ValueVector op (fn [name & args]
                            (vec (cons name (map type args)))))

(defmacro defop [name & op-signatures]
  `(do
     ~@(for [[signature expression inits] op-signatures]
         (let [arg-types (butlast signature)
               return-type (last signature)
               arg-syms (for [[^long n arg-type] (map-indexed vector arg-types)]
                          (with-meta (symbol (str (char (+ (int \a)  n)))) {:tag (maybe-primitive-type-sym arg-type)}))
               idx-sym 'idx]
           `(defmethod op ~(vec (cons name (map maybe-array-type-form arg-types))) ~(vec (cons '_ arg-syms))
              (let [out# (new ~return-type "" *allocator*)
                    value-count# (.getValueCount ~(first arg-syms))
                    ~@inits]
                (.allocateNew out# value-count#)
                (dotimes [~idx-sym value-count#]
                  (.set out# ~idx-sym ~expression))
                (.setValueCount out# value-count#)
                out#))))))

(defop :+
  [[BaseIntVector Double Float8Vector]
   (+ (.getValueAsLong a idx) b)]
  [[BaseIntVector Long BigIntVector]
   (+ (.getValueAsLong a idx) b)]
  [[BaseIntVector BaseIntVector BigIntVector]
   (+ (.getValueAsLong a idx)
      (.getValueAsLong b idx))]
  [[BaseIntVector FloatingPointVector Float8Vector]
   (+ (.getValueAsLong a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector Double Float8Vector]
   (+ (.getValueAsDouble a idx) b)]
  [[FloatingPointVector Long Float8Vector]
   (+ (.getValueAsDouble a idx) b)]
  [[FloatingPointVector FloatingPointVector Float8Vector]
   (+ (.getValueAsDouble a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector BaseIntVector Float8Vector]
   (+ (.getValueAsDouble a idx)
      (.getValueAsLong b idx))])

(defop :-
  [[BaseIntVector BigIntVector]
   (- (.getValueAsLong a idx))]
  [[FloatingPointVector Float8Vector]
   (- (.getValueAsDouble a idx))]
  [[BaseIntVector Double Float8Vector]
   (- (.getValueAsLong a idx) b)]
  [[BaseIntVector Long BigIntVector]
   (- (.getValueAsLong a idx) b)]
  [[BaseIntVector BaseIntVector BigIntVector]
   (- (.getValueAsLong a idx)
      (.getValueAsLong b idx))]
  [[BaseIntVector FloatingPointVector Float8Vector]
   (- (.getValueAsLong a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector Double Float8Vector]
   (- (.getValueAsDouble a idx) b)]
  [[FloatingPointVector Long Float8Vector]
   (- (.getValueAsDouble a idx) b)]
  [[FloatingPointVector FloatingPointVector Float8Vector]
   (- (.getValueAsDouble a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector BaseIntVector Float8Vector]
   (- (.getValueAsDouble a idx)
      (.getValueAsLong b idx))])

(defop :*
  [[BaseIntVector Double Float8Vector]
   (* (.getValueAsLong a idx) b)]
  [[BaseIntVector Long BigIntVector]
   (* (.getValueAsLong a idx) b)]
  [[BaseIntVector BaseIntVector BigIntVector]
   (* (.getValueAsLong a idx)
      (.getValueAsLong b idx))]
  [[BaseIntVector FloatingPointVector Float8Vector]
   (* (.getValueAsLong a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector Double Float8Vector]
   (* (.getValueAsDouble a idx) b)]
  [[FloatingPointVector Long Float8Vector]
   (* (.getValueAsDouble a idx) b)]
  [[FloatingPointVector FloatingPointVector Float8Vector]
   (* (.getValueAsDouble a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector BaseIntVector Float8Vector]
   (* (.getValueAsDouble a idx)
      (.getValueAsLong b idx))])

(defop :/
  [[BaseIntVector Double Float8Vector]
   (/ (.getValueAsLong a idx) b)]
  [[BaseIntVector Long BigIntVector]
   (quot (.getValueAsLong a idx) b)]
  [[BaseIntVector BaseIntVector BigIntVector]
   (quot (.getValueAsLong a idx)
         (.getValueAsLong b idx))]
  [[BaseIntVector FloatingPointVector Float8Vector]
   (/ (.getValueAsLong a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector Double Float8Vector]
   (/ (.getValueAsDouble a idx) b)]
  [[FloatingPointVector Long Float8Vector]
   (/ (.getValueAsDouble a idx) b)]
  [[FloatingPointVector FloatingPointVector Float8Vector]
   (/ (.getValueAsDouble a idx)
      (.getValueAsDouble b idx))]
  [[FloatingPointVector BaseIntVector Float8Vector]
   (/ (.getValueAsDouble a idx)
      (.getValueAsLong b idx))])

(defmacro boolean->bit [b]
  `(if ~b 1 0))

(defop :=
  [[BaseIntVector Double BitVector]
   (boolean->bit (== (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (= (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (== (.getValueAsLong a idx)
                     (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (= (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (== (.getValueAsDouble a idx) b))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (= (.getDataPointer a idx a-pointer)
                    (.getDataPointer b idx b-pointer)))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (= (.getDataPointer a idx a-pointer) b))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (= (str (.getObject a idx)) b))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (Arrays/equals (.get a idx) b))]
  [[BitVector BitVector BitVector]
   (boolean->bit (= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (= (.get a idx) (boolean->bit b)))])

(defop :!=
  [[BaseIntVector Double BitVector]
   (boolean->bit (not= (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (not= (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (not= (.getValueAsLong a idx)
                       (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (not= (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (not= (.getValueAsDouble a idx) b))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not= (.getDataPointer a idx a-pointer)
                       (.getDataPointer b idx b-pointer)))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not= (.getDataPointer a idx a-pointer) b))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not= (str (.getObject a idx)) b))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (Arrays/equals (.get a idx) b)))]
  [[BitVector BitVector BitVector]
   (boolean->bit (not= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (not= (.get a idx) (boolean->bit b)))])

(defop :<
  [[BaseIntVector Double BitVector]
   (boolean->bit (< (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (< (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (< (.getValueAsLong a idx)
                    (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (< (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (< (.getValueAsDouble a idx) b))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (< (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (< (.get a idx) (.getTime b)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (neg? (.compareTo (.getDataPointer a idx a-pointer)
                                   (.getDataPointer b idx b-pointer))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (neg? (.compareTo (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (neg? (.compareTo (str (.getObject a idx)) b)))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (neg? (Arrays/compareUnsigned (.get a idx) b)))]
  [[BitVector BitVector BitVector]
   (boolean->bit (< (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (< (.get a idx) (boolean->bit b)))])

(defop :<=
  [[BaseIntVector Double BitVector]
   (boolean->bit (<= (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (<= (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (<= (.getValueAsLong a idx)
                    (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (<= (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (<= (.getValueAsDouble a idx) b))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (<= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (<= (.get a idx) (.getTime b)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not (pos? (.compareTo (.getDataPointer a idx a-pointer)
                                        (.getDataPointer b idx b-pointer)))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not (pos? (.compareTo (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not (pos? (.compareTo (str (.getObject a idx)) b))))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (pos? (Arrays/compareUnsigned (.get a idx) b))))]
  [[BitVector BitVector BitVector]
   (boolean->bit (<= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (<= (.get a idx) (boolean->bit b)))])

(defop :>
  [[BaseIntVector Double BitVector]
   (boolean->bit (> (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (> (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (> (.getValueAsLong a idx)
                    (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (> (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (> (.getValueAsDouble a idx) b))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (> (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (> (.get a idx) (.getTime b)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (pos? (.compareTo (.getDataPointer a idx a-pointer)
                                   (.getDataPointer b idx b-pointer))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (pos? (.compareTo (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (pos? (.compareTo (str (.getObject a idx)) b)))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (pos? (Arrays/compareUnsigned (.get a idx) b)))]
  [[BitVector BitVector BitVector]
   (boolean->bit (> (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (> (.get a idx) (boolean->bit b)))])

(defop :>=
  [[BaseIntVector Double BitVector]
   (boolean->bit (>= (.getValueAsLong a idx) b))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (>= (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (>= (.getValueAsLong a idx)
                    (.getValueAsDouble b idx)))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (>= (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (>= (.getValueAsDouble a idx) b))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (>= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (>= (.get a idx) (.getTime b)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not (neg? (.compareTo (.getDataPointer a idx a-pointer)
                                        (.getDataPointer b idx b-pointer)))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not (neg? (.compareTo (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not (neg? (.compareTo (str (.getObject a idx)) b))))]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (neg? (Arrays/compareUnsigned (.get a idx) b))))]
  [[BitVector BitVector BitVector]
   (boolean->bit (>= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (>= (.get a idx) (boolean->bit b)))])

(defop :not
  [[BitVector BitVector]
   (if (= 1 (.get a idx)) 0 1)])

(defop :and
  [[BitVector BitVector BitVector]
   (boolean->bit (= 1 (.get a idx) (.get b idx)))])

(defop :or
  [[BitVector BitVector BitVector]
   (boolean->bit (or (= 1 (.get a idx)) (= 1 (.get b idx))))])

(defop :udf
  [[BaseIntVector LongPredicate BitVector]
   (boolean->bit (.test b (.getValueAsLong a idx)))]
  [[BaseIntVector DoublePredicate BitVector]
   (boolean->bit (.test b (.getValueAsLong a idx)))]
  [[FloatingPointVector LongPredicate BitVector]
   (boolean->bit (.test b (.getValueAsDouble a idx)))]
  [[FloatingPointVector DoublePredicate BitVector]
   (boolean->bit (.test b (.getValueAsDouble a idx)))]
  [[ValueVector Predicate BitVector]
   (boolean->bit (.test b (.getObject a idx)))]
  [[BaseIntVector LongUnaryOperator BigIntVector]
   (.applyAsLong b (.getValueAsLong a idx))]
  [[BaseIntVector DoubleUnaryOperator Float8Vector]
   (.applyAsDouble b (.getValueAsLong a idx))]
  [[BaseIntVector LongToDoubleFunction Float8Vector]
   (.applyAsDouble b (.getValueAsLong a idx))]
  [[ValueVector ToLongFunction BigIntVector]
   (.applyAsLong b (.getObject a idx))]
  [[Float8Vector LongUnaryOperator BigIntVector]
   (.applyAsLong b (.getValueAsDouble a idx))]
  [[Float8Vector DoubleUnaryOperator Float8Vector]
   (.applyAsDouble b (.getValueAsDouble a idx))]
  [[Float8Vector DoubleToLongFunction BigIntVector]
   (.applyAsLong b (.getValueAsDouble a idx))]
  [[ValueVector ToDoubleFunction Float8Vector]
   (.applyAsDouble b (.getObject a idx))]
  [[BitVector Function BitVector]
   (boolean->bit (.apply b (= 1 (.get a idx))))]
  [[TimeStampVector LongUnaryOperator TimeStampMilliVector]
   (.applyAsLong b (.get a idx))]
  [[TimeStampVector Function TimeStampMilliVector]
   (.getTime ^Date (.apply b (Date. (.get a idx))))]
  [[VarCharVector Function VarCharVector]
   (Text. (str (.apply b (str (.get a idx)))))]
  [[VarBinaryVector Function VarBinaryVector]
   ^bytes (.apply b (.get a idx))])

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
                  is+f (op :+ is 2.0)
                  is+i (op :+ is 2)
                  is+fs (op :+ is fs)
                  is+is (op :+ is is)
                  fs+i (op :+ fs 2)
                  fs+f (op :+ fs 2.0)
                  fs+is (op :+ fs is)
                  fs+fs (op :+ fs fs)]

        (t/is (= [3.0 4.0 5.0] (tu/->list is+f)))
        (t/is (= [3 4 5] (tu/->list is+i)))
        (t/is (= [2.0 4.0 6.0] (tu/->list is+fs)))
        (t/is (= [2 4 6] (tu/->list is+is)))

        (t/is (= [3.0 4.0 5.0] (tu/->list fs+i)))
        (t/is (= [3.0 4.0 5.0] (tu/->list fs+f)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+is)))
        (t/is (= [2.0 4.0 6.0] (tu/->list fs+fs)))))))
