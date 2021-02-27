(ns core2.compute
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers]
           [org.apache.arrow.vector BaseIntVector BaseVariableWidthVector BigIntVector BitVector ElementAddressableVector
            FloatingPointVector Float8Vector TimeStampVector TimeStampMilliVector VarBinaryVector VarCharVector ValueVector]
           org.apache.arrow.vector.util.Text
           [java.util.function DoublePredicate LongPredicate Predicate DoubleBinaryOperator LongBinaryOperator
            DoubleUnaryOperator LongUnaryOperator LongToDoubleFunction DoubleToLongFunction Function
            ToDoubleFunction ToLongFunction ToDoubleBiFunction ToLongBiFunction]
           java.lang.reflect.Modifier
           java.nio.charset.StandardCharsets
           [java.util Arrays Date]))

;; Arrow compute kernels spike, loosely based on
;; https://arrow.apache.org/docs/cpp/compute.html

;; TODO:
;; scalar first operations

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic ^BufferAllocator *allocator*)

(defn- maybe-primitive-type-sym [c]
  (get '{Double double Long long} c c))

(defn- maybe-array-type-form [c]
  (case c
    bytes `(Class/forName "[B")
    c))

(defn- vector-type? [arg-type-sym]
  (let [resolved (resolve arg-type-sym)]
    (and (instance? Class resolved)
         (.isAssignableFrom ValueVector resolved))))

(defmulti op (fn [name & args]
               (vec (cons name (map type (if (map? (last args))
                                           (butlast args)
                                           args))))))

(defn- validate-value-count
  ([^ValueVector a] (.getValueCount a))
  ([^ValueVector a ^ValueVector b]
   (let [a-count (.getValueCount a)
         b-count (.getValueCount b)]
     (assert (= a-count b-count))
     a-count))
  ([a b & vecs]
   (let [vcs (for [^ValueVector vec (cons a (cons b vecs))]
               (.getValueCount vec))]
     (assert (= 1 (count (distinct vcs))))
     (first vcs))))

(defn- op-type [arg-types return-type]
  (cond
    (and (vector-type? return-type)) ::map
    (and (= 1 (count arg-types))
         (vector-type? (last arg-types))) ::reduce
    (and (= 2 (count arg-types))
         (not (vector-type? (first arg-types)))
         (vector-type? (last arg-types))) ::reduce-init))

(defmacro defop-overload [name signature expression inits]
  (let [arg-types (butlast signature)
        return-type (last signature)
        raw-arg-syms (for [[^long n arg-type] (map-indexed vector arg-types)]
                       (symbol (str (char (+ (int \a)  n)))))
        arg-syms (for [[arg-sym arg-type] (map vector raw-arg-syms arg-types)]
                   (with-meta arg-sym {:tag (maybe-primitive-type-sym arg-type)}))
        args-init (mapcat vector raw-arg-syms arg-syms)
        idx-sym 'idx
        acc-sym 'acc
        out-sym (with-meta 'out {:tag return-type})
        vec-syms (vec (for [[arg-sym arg-type] (map vector arg-syms arg-types)
                            :when (vector-type? arg-type)]
                        arg-sym))
        op-type (op-type arg-types return-type)
        option-map {:keys [out-sym]}
        real-args (cond-> (vec (cons '_ raw-arg-syms))
                    (= ::map op-type) (-> (conj '&) (conj [option-map])))
        dispatch-value (vec (cons name (map maybe-array-type-form arg-types)))]
    `(defmethod op ~dispatch-value ~real-args
       ~(case op-type
          ::map
          `(let [~@args-init
                 out# ~out-sym
                 ~acc-sym (or out#
                              ~(let [mods (.getModifiers ^Class (resolve return-type))]
                                 (when-not (or (Modifier/isAbstract mods)
                                               (Modifier/isInterface mods))
                                   `(new ~return-type "" *allocator*))))
                 value-count# (validate-value-count ~@vec-syms)
                 ~@inits]
             (when out#
               (.clear ~acc-sym))
             (.allocateNew ~acc-sym value-count#)
             (dotimes [~idx-sym value-count#]
               (.set ~acc-sym ~idx-sym ~expression))
             (.setValueCount ~acc-sym value-count#)
             ~acc-sym)

          ::reduce
          `(let [~@args-init
                 value-count# (validate-value-count ~@vec-syms)
                 ~@inits]
             (loop [~idx-sym (int 0)
                    ~acc-sym ~expression]
               (let [~idx-sym (inc ~idx-sym)]
                 (if (= value-count# ~idx-sym)
                   ~acc-sym
                   (recur ~idx-sym ~expression)))))

          ::reduce-init
          (let [cast-acc? (and (not= (maybe-primitive-type-sym return-type) return-type)
                               (not= return-type (first arg-types)))
                acc-sym (with-meta (first arg-syms) {})]
            `(let [~@args-init
                   value-count# (validate-value-count ~@vec-syms)
                   ~@inits]
               (loop [~idx-sym (int 0)
                      ~acc-sym ~(cond->> acc-sym
                                  cast-acc? (list (maybe-primitive-type-sym return-type)))]
                 (if (= value-count# ~idx-sym)
                   ~acc-sym
                   (recur (inc ~idx-sym) ~(cond->> expression
                                            cast-acc? (list (maybe-primitive-type-sym return-type))))))))

          (throw (UnsupportedOperationException.))))))

(defmacro defop [name & op-signatures]
  `(do ~@(for [[signature expression inits] op-signatures]
           `(defop-overload ~name ~signature ~expression ~inits))))

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

(set! *unchecked-math* true)

(defmethod op [:+ Number Number] [_ a b]
  (+ a b))

(defmethod op [:- Number] [_ a]
  (- a))

(defmethod op [:- Number Number] [_ a b]
  (- a b))

(defmethod op [:* Number Number] [_ a b]
  (* a b))

(defmethod op [:/ Number Number] [_ a b]
  (let [x (/ a b)]
    (cond-> x
      (ratio? x) (double))))

(set! *unchecked-math* :warn-on-boxed)

(defmacro boolean->bit [b]
  `(if ~b 1 0))

(defmacro compare-pointer-to-bytes [a b]
  `(let [a# ~a
         b# ~b]
     (ByteFunctionHelpers/compare (.getBuf a#) (.getOffset a#) (+ (.getOffset a#) (.getLength a#))
                                  b# 0 (alength b#))))

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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (== (.getValueAsDouble a idx)
                     (.getValueAsLong b idx)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (= (.getDataPointer a idx a-pointer)
                    (.getDataPointer b idx b-pointer)))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (= (.getDataPointer a idx a-pointer) b))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (zero? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (zero? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
  [[BitVector BitVector BitVector]
   (boolean->bit (= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (= (.get a idx) (boolean->bit b)))])

(defop :!=
  [[BaseIntVector Double BitVector]
   (boolean->bit (not (== (.getValueAsLong a idx) b)))]
  [[BaseIntVector Long BitVector]
   (boolean->bit (not= (.getValueAsLong a idx) b))]
  [[BaseIntVector FloatingPointVector BitVector]
   (boolean->bit (not (== (.getValueAsLong a idx)
                          (.getValueAsDouble b idx))))]
  [[FloatingPointVector Double BitVector]
   (boolean->bit (not= (.getValueAsDouble a idx) b))]
  [[FloatingPointVector Long BitVector]
   (boolean->bit (not (== (.getValueAsDouble a idx) b)))]
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (not (== (.getValueAsDouble a idx)
                          (.getValueAsLong b idx))))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not= (.getDataPointer a idx a-pointer)
                       (.getDataPointer b idx b-pointer)))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not= (.getDataPointer a idx a-pointer) b))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not (zero? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (zero? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[BitVector BitVector BitVector]
   (boolean->bit (not= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (not= (.get a idx) (boolean->bit b)))])

(def ^:private bytes-class (Class/forName "[B"))

(defmethod op [:= Object Object] [_ a b]
  (= a b))

(defmethod op [:!= Object Object] [_ a b]
  (not= a b))

(set! *unchecked-math* true)

(defmethod op [:= Number Number] [_ a b]
  (== a b))

(defmethod op [:!= Number Number] [_ a b]
  (not (== a b)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod op [:= bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (Arrays/equals a b))

(defmethod op [:!= bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (not (Arrays/equals a b)))

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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (< (.getValueAsDouble a idx)
                    (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (< (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (< (.get a idx) b))
   [b (.getTime b)]]
  [[TimeStampVector TimeStampVector BitVector]
   (boolean->bit (< (.get a idx) (.get b idx)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (neg? (.compareTo (.getDataPointer a idx a-pointer)
                                   (.getDataPointer b idx b-pointer))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (neg? (.compareTo (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (neg? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (neg? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (<= (.getValueAsDouble a idx)
                     (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (<= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (<= (.get a idx) b))
   [b (.getTime b)]]
  [[TimeStampVector TimeStampVector BitVector]
   (boolean->bit (<= (.get a idx) (.get b idx)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not (pos? (.compareTo (.getDataPointer a idx a-pointer)
                                        (.getDataPointer b idx b-pointer)))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not (pos? (.compareTo (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not (pos? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (pos? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (> (.getValueAsDouble a idx)
                    (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (> (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (> (.get a idx) b))
   [b (.getTime b)]]
  [[TimeStampVector TimeStampVector BitVector]
   (boolean->bit (> (.get a idx) (.get b idx)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (pos? (.compareTo (.getDataPointer a idx a-pointer)
                                   (.getDataPointer b idx b-pointer))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (pos? (.compareTo (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (pos? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (pos? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b)))
   [a-pointer (ArrowBufPointer.)]]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (>= (.getValueAsDouble a idx)
                     (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (>= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (>= (.get a idx) b))
   [b (.getTime b)]]
  [[TimeStampVector TimeStampVector BitVector]
   (boolean->bit (>= (.get a idx) (.get b idx)))]
  [[ElementAddressableVector ElementAddressableVector BitVector]
   (boolean->bit (not (neg? (.compareTo (.getDataPointer a idx a-pointer)
                                        (.getDataPointer b idx b-pointer)))))
   [a-pointer (ArrowBufPointer.)
    b-pointer (ArrowBufPointer.)]]
  [[ElementAddressableVector ArrowBufPointer BitVector]
   (boolean->bit (not (neg? (.compareTo (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[VarCharVector String BitVector]
   (boolean->bit (not (neg? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)
    b (.getBytes ^String b StandardCharsets/UTF_8)]]
  [[VarBinaryVector bytes BitVector]
   (boolean->bit (not (neg? (compare-pointer-to-bytes (.getDataPointer a idx a-pointer) b))))
   [a-pointer (ArrowBufPointer.)]]
  [[BitVector BitVector BitVector]
   (boolean->bit (>= (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (>= (.get a idx) (boolean->bit b)))])

(set! *unchecked-math* true)

(defmethod op [:< Number Number] [_ a b]
  (< a b))

(defmethod op [:<= Number Number] [_ a b]
  (<= a b))

(defmethod op [:> Number Number] [_ a b]
  (> a b))

(defmethod op [:>= Number Number] [_ a b]
  (>= a b))

(set! *unchecked-math* :warn-on-boxed)

(defmethod op [:< Comparable Comparable] [_ ^Comparable a ^Comparable b]
  (neg? (.compareTo a b)))

(defmethod op [:<= Comparable Comparable] [_ ^Comparable a ^Comparable b]
  (not (pos? (.compareTo a b))))

(defmethod op [:> Comparable Comparable] [_ ^Comparable a ^Comparable b]
  (pos? (.compareTo a b)))

(defmethod op [:>= Comparable Comparable] [_ ^Comparable a ^Comparable b]
  (not (neg? (.compareTo a b))))

(defmethod op [:< bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (neg? (Arrays/compareUnsigned a b)))

(defmethod op [:<= bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (not (pos? (Arrays/compareUnsigned a b))))

(defmethod op [:> bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (pos? (Arrays/compareUnsigned a b)))

(defmethod op [:>= bytes-class bytes-class] [_ ^bytes a ^bytes b]
  (not (neg? (Arrays/compareUnsigned a b))))

(defop :not
  [[BitVector BitVector]
   (if (= 1 (.get a idx)) 0 1)])

(defop :and
  [[BitVector BitVector BitVector]
   (boolean->bit (= 1 (.get a idx) (.get b idx)))]
  [[BitVector Boolean BitVector]
   (boolean->bit (= 1 (.get a idx) b))
   [b (boolean->bit b)]]
  [[BitVector Number BitVector]
   (boolean->bit (= 1 (.get a idx) b))
   [^long b b]])

(defop :or
  [[BitVector BitVector BitVector]
   (boolean->bit (or (= 1 (.get a idx)) (= 1 (.get b idx))))]
  [[BitVector Boolean BitVector]
   (boolean->bit (or (= 1 (.get a idx)) (= 1 b)))
   [b (boolean->bit b)]]
  [[BitVector Number BitVector]
   (boolean->bit (or (= 1 (.get a idx)) (= 1 b)))
   [^long b b]])

(defmethod op [:not Boolean] [_ a]
  (not a))

(defmethod op [:and Boolean Boolean] [_ a b]
  (and a b))

(defmethod op [:or Boolean Boolean] [_ a b]
  (or a))

(defmethod op [:not Number] [_ a]
  (if (= 1 a) 0 1))

(defmethod op [:and Number Number] [_ a b]
  (= 1 a b))

(defmethod op [:or Number Number] [_ a b]
  (or (= 1 a) (= 1 b)))

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
  [[BaseIntVector BaseIntVector LongBinaryOperator BigIntVector]
   (.applyAsLong c (.getValueAsLong a idx) (.getValueAsLong b idx))]
  [[BaseIntVector BaseIntVector DoubleBinaryOperator Float8Vector]
   (.applyAsDouble c (.getValueAsLong a idx) (.getValueAsLong b idx))]
  [[ValueVector ToLongFunction BigIntVector]
   (.applyAsLong b (.getObject a idx))]
  [[ValueVector ValueVector ToLongBiFunction BigIntVector]
   (.applyAsLong c (.getObject a idx) (.getObject b idx))]
  [[FloatingPointVector LongUnaryOperator BigIntVector]
   (.applyAsLong b (.getValueAsDouble a idx))]
  [[FloatingPointVector DoubleUnaryOperator Float8Vector]
   (.applyAsDouble b (.getValueAsDouble a idx))]
  [[FloatingPointVector DoubleToLongFunction BigIntVector]
   (.applyAsLong b (.getValueAsDouble a idx))]
  [[FloatingPointVector FloatingPointVector LongBinaryOperator BigIntVector]
   (.applyAsLong c (.getValueAsDouble a idx) (.getValueAsDouble b idx))]
  [[FloatingPointVector FloatingPointVector DoubleBinaryOperator Float8Vector]
   (.applyAsDouble c (.getValueAsDouble a idx) (.getValueAsDouble b idx))]
  [[ValueVector ToDoubleFunction Float8Vector]
   (.applyAsDouble b (.getObject a idx))]
  [[ValueVector ValueVector ToDoubleBiFunction Float8Vector]
   (.applyAsDouble c (.getObject a idx) (.getObject b idx))]
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

(defop :sum
  [[BaseIntVector Long]
   (+ acc (.getValueAsLong a idx))
   [acc 0]]
  [[FloatingPointVector Double]
   (+ acc (.getValueAsDouble a idx))
   [acc 0.0]]
  [[Long BaseIntVector Long]
   (+ a (.getValueAsLong b idx))]
  [[Double BaseIntVector Long]
   (+ a (.getValueAsLong b idx))]
  [[Long FloatingPointVector Double]
   (+ a (.getValueAsDouble b idx))]
  [[Double FloatingPointVector Double]
   (+ a (.getValueAsDouble b idx))])

(defop :min
  [[BaseIntVector Long]
   (min acc (.getValueAsLong a idx))
   [acc Long/MAX_VALUE]]
  [[FloatingPointVector Double]
   (min acc (.getValueAsDouble a idx))
   [acc Double/MAX_VALUE]]
  [[Long BaseIntVector Long]
   (min a (.getValueAsLong b idx))]
  [[Double BaseIntVector Long]
   (min a (.getValueAsLong b idx))]
  [[Long FloatingPointVector Double]
   (min a (.getValueAsDouble b idx))]
  [[Double FloatingPointVector Double]
   (min a (.getValueAsDouble b idx))]
  [[VarBinaryVector bytes]
   (let [x (.get a idx)]
     (cond
       (nil? acc) x
       (neg? (Arrays/compareUnsigned acc x)) acc
       :else x))
   [^bytes acc nil]]
  [[bytes VarBinaryVector bytes]
   (let [x (.get b idx)]
     (if (neg? (Arrays/compareUnsigned a x))
       a
       x))]
  [[VarCharVector String]
   (let [x (str (.getObject a idx))]
     (cond
       (nil? acc) x
       (neg? (.compareTo acc x)) acc
       :else x))
   [^String acc nil]]
  [[String VarCharVector String]
   (let [x (str (.getObject b idx))]
     (if (neg? (.compareTo a x))
       a
       x))]
  [[TimeStampVector Date]
   (let [x (.get a idx)]
     (cond
       (nil? acc) (Date. x)
       (< (.getTime acc) x) acc
       :else (Date. x)))
   [^Date acc nil]]
  [[Date TimeStampVector Date]
   (let [x (.get b idx)]
     (if (< (.getTime a) x)
       a
       (Date. x)))]
  [[ValueVector Comparable]
   (let [x ^Comparable (.getObject a idx)]
     (cond
       (nil? acc) x
       (neg? (.compareTo acc x)) acc
       :else x))
   [^Comparable acc nil]]
  [[Comparable ValueVector Comparable]
   (let [x ^Comparable (.getObject b idx)]
     (if (neg? (.compareTo a x))
       a
       x))])

(defop :max
  [[BaseIntVector Long]
   (max acc (.getValueAsLong a idx))
   [acc Long/MIN_VALUE]]
  [[FloatingPointVector Double]
   (max acc (.getValueAsDouble a idx))
   [acc Double/MIN_VALUE]]
  [[Long BaseIntVector Long]
   (max a (.getValueAsLong b idx))]
  [[Double BaseIntVector Long]
   (max a (.getValueAsLong b idx))]
  [[Long FloatingPointVector Double]
   (max a (.getValueAsDouble b idx))]
  [[Double FloatingPointVector Double]
   (max a (.getValueAsDouble b idx))]
  [[VarBinaryVector bytes]
   (let [x (.get a idx)]
     (cond
       (nil? acc) x
       (neg? (Arrays/compareUnsigned acc x)) x
       :else acc))
   [^bytes acc nil]]
  [[bytes VarBinaryVector bytes]
   (let [x (.get b idx)]
     (if (neg? (Arrays/compareUnsigned a x))
       x
       a))]
  [[VarCharVector String]
   (let [x (str (.getObject a idx))]
     (cond
       (nil? acc) x
       (neg? (.compareTo acc x)) x
       :else acc))
   [^String acc nil]]
  [[String VarCharVector String]
   (let [x (str (.getObject b idx))]
     (if (neg? (.compareTo a x))
       x
       a))]
  [[TimeStampVector Date]
   (let [x (.get a idx)]
     (cond
       (nil? acc) (Date. x)
       (< (.getTime acc) x) (Date. x)
       :else acc))
   [^Date acc nil]]
  [[Date TimeStampVector Date]
   (let [x (.get b idx)]
     (if (< (.getTime a) x)
       (Date. x)
       a))]
  [[ValueVector Comparable]
   (let [x ^Comparable (.getObject a idx)]
     (cond
       (nil? acc) x
       (neg? (.compareTo acc x)) x
       :else acc))
   [^Comparable acc nil]]
  [[Comparable ValueVector Comparable]
   (let [x ^Comparable (.getObject b idx)]
     (if (neg? (.compareTo a x))
       x
       a))])

(defmethod op [:count ValueVector] [_ ^ValueVector a]
  (.getValueCount a))

(defmethod op [:count Long ValueVector] [_ ^long a ^ValueVector b]
  (+ a (.getValueCount b)))

(set! *unchecked-math* true)

(definterface IAverage
  (^Number numerator [])
  (^Number denominator []))

(defn- ->avarge [^Number numerator ^Number denominator]
  (proxy [Number IAverage Comparable] []
    (doubleValue [] (double (/ numerator denominator)))
    (floatValue [] (.doubleValue ^Number this))
    (intValue [] (.doubleValue ^Number this))
    (longValue [] (.doubleValue ^Number this))
    (numerator [] numerator)
    (denominator [] denominator)
    (toString [] (str numerator "/" denominator))
    (equals [other] (and (instance? Number other)
                         (== (.doubleValue ^Number this) (.doubleValue ^Number other))))
    (comapreTo [other] (and (instance? Number other)
                            (Double/compare (.doubleValue ^Number this) (.doubleValue ^Number other))))))

(defmethod op [:avg BaseIntVector] [_ ^BaseIntVector a]
  (->avarge (op :sum a) (op :count a)))

(defmethod op [:avg IAverage BaseIntVector] [_ ^IAverage a ^BaseIntVector b]
  (->avarge (+ (.numerator a) (op :sum b))
            (+ (.denominator a) (op :count b))))

(defmethod op [:avg FloatingPointVector] [_ ^FloatingPointVector a]
  (->avarge (op :sum a) (op :count a)))

(defmethod op [:avg IAverage FloatingPointVector] [_ ^IAverage a ^FloatingPointVector b]
  (->avarge (+ (.numerator a) (op :sum b))
            (+ (.denominator a) (op :count b))))

(set! *unchecked-math* false)

(defmethod op [:filter ValueVector BitVector] [_ ^ValueVector a ^BitVector b]
  (let [out (.createVector (.getField a) *allocator*)]
    (dotimes [n (.getValueCount a)]
      (when (= 1 (.get b n))
        (let [value-count (.getValueCount out)]
          (.copyFromSafe out n value-count a)
          (.setValueCount out (inc value-count)))))
    out))

(defn- try-enable-simd []
  (try
    (require 'core2.compute.simd)
    (log/info "SIMD enabled")
    true
    (catch clojure.lang.Compiler$CompilerException e
      (if (instance? ClassNotFoundException (.getCause e))
        (log/info "SIMD not enabled")
        (throw e))
      false)))

(defonce simd-enabled? (try-enable-simd))
