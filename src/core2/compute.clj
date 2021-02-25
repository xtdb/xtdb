(ns core2.compute
  (:import [org.apache.arrow.memory BufferAllocator]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector BaseIntVector BaseVariableWidthVector BigIntVector BitVector ElementAddressableVector
            FloatingPointVector Float8Vector TimeStampVector TimeStampMilliVector VarBinaryVector VarCharVector ValueVector]
           org.apache.arrow.vector.util.Text
           [java.util.function DoublePredicate LongPredicate Predicate DoubleBinaryOperator LongBinaryOperator
            DoubleUnaryOperator LongUnaryOperator LongToDoubleFunction DoubleToLongFunction Function
            ToDoubleFunction ToLongFunction ToDoubleBiFunction ToLongBiFunction]
           java.lang.reflect.Modifier
           [java.util Arrays Date]))

;; Arrow compute kernels spike, loosely based on
;; https://arrow.apache.org/docs/cpp/compute.html

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic ^BufferAllocator *allocator*)

(defn- maybe-primitive-type-sym [c]
  (get '{Double double Long long} c c))

(defn- maybe-array-type-form [c]
  (case c
    bytes `(Class/forName "[B")
    c))

(defmulti op (fn [name & args]
               (vec (cons name (map type args)))))

(defmacro defop [name & op-signatures]
  `(do
     ~@(for [[signature expression inits] op-signatures]
         (let [arg-types (butlast signature)
               return-type (last signature)
               arg-syms (for [[^long n arg-type] (map-indexed vector arg-types)]
                          (with-meta (symbol (str (char (+ (int \a)  n)))) {:tag (maybe-primitive-type-sym arg-type)}))
               idx-sym 'idx
               acc-sym 'acc
               resolved-return-type (resolve return-type)]
           `(defmethod op ~(vec (cons name (map maybe-array-type-form arg-types))) ~(vec (cons '_ arg-syms))
              ~(cond
                 (and (instance? Class resolved-return-type)
                      (.isAssignableFrom ValueVector resolved-return-type))
                 `(let [~acc-sym ~(let [mods (.getModifiers ^Class resolved-return-type)]
                                    (when-not (or (Modifier/isAbstract mods)
                                                  (Modifier/isInterface mods))
                                      `(new ~return-type "" *allocator*)))
                        value-count# (.getValueCount ~(first arg-syms))
                        ~@inits]
                    (.allocateNew ~acc-sym value-count#)
                    (dotimes [~idx-sym value-count#]
                      (.set ~acc-sym ~idx-sym ~expression))
                    (.setValueCount ~acc-sym value-count#)
                    ~acc-sym)

                 (= 1 (count arg-types))
                 `(let [value-count# (.getValueCount ~(last arg-syms))
                        ~@inits]
                    (loop [~idx-sym (int 0)
                           ~acc-sym ~expression]
                      (let [~idx-sym (inc ~idx-sym)]
                        (if (= value-count# ~idx-sym)
                          ~acc-sym
                          (recur ~idx-sym ~expression)))))

                 :else
                 (let [cast-acc? (and (not= (maybe-primitive-type-sym return-type) return-type)
                                      (not= return-type (first arg-types)))
                       acc-sym (with-meta (first arg-syms) {})]
                   `(let [value-count# (.getValueCount ~(last arg-syms))
                          ~@inits]
                      (loop [~idx-sym (int 0)
                             ~acc-sym ~(cond->> acc-sym
                                         cast-acc? (list (maybe-primitive-type-sym return-type)))]
                        (if (= value-count# ~idx-sym)
                          ~acc-sym
                          (recur (inc ~idx-sym) ~(cond->> expression
                                                   cast-acc? (list (maybe-primitive-type-sym return-type))))))))))))))

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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (not= (.getValueAsDouble a idx)
                       (.getValueAsLong b idx)))]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (< (.getValueAsDouble a idx)
                    (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (< (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (< (.get a idx) (.getTime b)))]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (<= (.getValueAsDouble a idx)
                     (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (<= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (<= (.get a idx) (.getTime b)))]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (> (.getValueAsDouble a idx)
                    (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (> (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (> (.get a idx) (.getTime b)))]
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
  [[FloatingPointVector BaseIntVector BitVector]
   (boolean->bit (>= (.getValueAsDouble a idx)
                     (.getValueAsLong b idx)))]
  [[TimeStampVector Long BitVector]
   (boolean->bit (>= (.get a idx) b))]
  [[TimeStampVector Date BitVector]
   (boolean->bit (>= (.get a idx) (.getTime b)))]
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

(defmethod op [:avg BaseIntVector] [_ ^BaseIntVector a]
  (double (/ ^long (op :sum a) ^long (op :count a))))

(defmethod op [:avg FloatingPointVector] [_ ^FloatingPointVector a]
  (/ ^double (op :sum a) ^long (op :count a)))
