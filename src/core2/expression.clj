(ns core2.expression
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            core2.operator.project
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           core2.operator.project.ProjectionSpec
           [core2.select IVectorSchemaRootSelector IVectorSelector]
           java.lang.reflect.Method
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Instant ZoneOffset]
           java.time.temporal.ChronoField
           java.util.Date
           [org.apache.arrow.vector BaseVariableWidthVector BitVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn expand-variadics [{:keys [op] :as expr}]
  (letfn [(expand-l [{:keys [f args]}]
            (reduce (fn [acc arg]
                      {:op :call, :f f, :args [acc arg]})
                    args))

          (expand-r [{:keys [f args]}]
            (reduce (fn [acc arg]
                      {:op :call, :f f, :args [arg acc]})
                    (reverse args)))]

    (or (when (= :call op)
          (let [{:keys [f args]} expr]
            (when (> (count args) 2)
              (cond
                (contains? '#{+ - * /} f) (expand-l expr)
                (contains? '#{and or} f) (expand-r expr)
                (contains? '#{<= < = != > >=} f) (expand-r {:op :call
                                                            :f 'and
                                                            :args (for [[x y] (partition 2 1 args)]
                                                                    {:op :call
                                                                     :f f
                                                                     :args [x y]})})))))

        expr)))

(defn form->expr [form]
  (cond
    (symbol? form) {:op :variable, :variable form}

    (sequential? form) (let [[f & args] form]
                         (case f
                           'if (do
                                 (when-not (= 3 (count args))
                                   (throw (IllegalArgumentException. (str "'if' expects 3 args: " (pr-str form)))))

                                 (let [[pred then else] args]
                                   {:op :if,
                                    :pred (form->expr pred),
                                    :then (form->expr then),
                                    :else (form->expr else)}))

                           (-> {:op :call, :f f, :args (mapv form->expr args)}
                               expand-variadics)))

    (string? form) (let [s (gensym "string-literal")]
                     {:op :literal
                      :literal form
                      :init-variable (with-meta s {:literal form})
                      :init-code `(ByteBuffer/wrap (.getBytes ~form StandardCharsets/UTF_8))})

    :else {:op :literal, :literal form}))

(defmulti postwalk-expr
  (fn [f {:keys [op] :as expr}]
    op))

(defmethod postwalk-expr :default [f expr]
  (f expr))

(defmethod postwalk-expr :if [f {:keys [pred then else]}]
  (f {:op :if
      :pred (postwalk-expr f pred)
      :then (postwalk-expr f then)
      :else (postwalk-expr f else)}))

(defmethod postwalk-expr :call [f {expr-f :f, :keys [args]}]
  (f {:op :call
      :f expr-f
      :args (mapv #(postwalk-expr f %) args)}))

(defn expr-seq [{:keys [op] :as expr}]
  (lazy-seq
   (case op
     :if (cons expr (mapcat (comp expr-seq expr) [:pred :then :else]))
     :call (cons expr (mapcat expr-seq (:args expr)))
     [expr])))

(defn with-tag [sym ^Class tag]
  (-> sym
      (vary-meta assoc :tag (symbol (.getName tag)))))

(defn variables [expr]
  (->> (expr-seq expr)
       (into [] (comp (filter (comp #(= :variable %) :op))
                      (map :variable)
                      (distinct)))))

(def type->cast
  {Long 'long
   Double 'double
   Date 'long
   Boolean 'boolean})

(def ^:private type->boxed-type {Double/TYPE Double
                                 Long/TYPE Long
                                 Boolean/TYPE Boolean})

(def idx-sym (gensym "idx"))

(def numeric-types
  #{Long Double})

(defn- widen-numeric-types [type-x type-y]
  (when (and (.isAssignableFrom Number type-x)
             (.isAssignableFrom Number type-y))
    (if (and (= type-x Long) (= type-y Long))
      Long
      Double)))

(defmulti codegen-expr
  (fn [{:keys [op]} var->type]
    op))

(def ^:dynamic *init-expressions*)

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x

    :else
    (:literal (meta x))))

(defmethod codegen-expr :literal [{:keys [literal init-variable]} _]
  (cond
    (instance? Date literal)
    {:code (.getTime ^Date literal)
     :return-type Date}
    (string? literal)
    {:code init-variable
     :return-type String}
    (nil? literal)
    {:code nil :return-type Comparable}
    :else
    {:code literal :return-type (class literal)}))

(defn element->nio-buffer ^java.nio.ByteBuffer [^ValueVector vec ^long idx]
  (let [value-buffer (.getDataBuffer vec)
        offset-buffer (.getOffsetBuffer vec)
        offset-idx (* idx BaseVariableWidthVector/OFFSET_WIDTH)
        offset (.getInt offset-buffer offset-idx)
        end-offset (.getInt offset-buffer (+ offset-idx BaseVariableWidthVector/OFFSET_WIDTH))]
    (.nioBuffer value-buffer offset (- end-offset offset))))

(defn compare-nio-buffers-unsigned ^long [^ByteBuffer x ^ByteBuffer y]
  (let [rem-x (.remaining x)
        rem-y (.remaining y)
        limit (min rem-x rem-y)
        char-limit (bit-shift-right limit 1)
        diff (.compareTo (.limit (.asCharBuffer x) char-limit)
                         (.limit (.asCharBuffer y) char-limit))]
    (if (zero? diff)
      (loop [n (bit-and-not limit 1)]
        (if (= n limit)
          (- rem-x rem-y)
          (let [x-byte (.get x n)
                y-byte (.get y n)]
            (if (= x-byte y-byte)
              (recur (inc n))
              (Byte/compareUnsigned x-byte y-byte)))))
      diff)))

(defmethod codegen-expr :variable [{:keys [variable]} {:keys [var->type]}]
  (let [type (or (get var->type variable)
                 (throw (IllegalArgumentException. (str "unknown variable: " variable))))]
    {:code (condp = type
             Boolean `(== 1 (.get ~variable ~idx-sym))
             String `(element->nio-buffer ~variable ~idx-sym)
             types/byte-array-class `(element->nio-buffer ~variable ~idx-sym)
             Comparable `(normalize-union-value (types/get-object ~variable ~idx-sym))
             `(.get ~variable ~idx-sym))
     :return-type type}))

(defmethod codegen-expr :if [{:keys [pred then else]} _]
  (let [{then-type :return-type} then
        {else-type :return-type} else
        return-type (if (= then-type else-type)
                      then-type
                      (or (widen-numeric-types then-type else-type)
                          Comparable))
        cast (get type->cast return-type)]
    {:code (cond->> `(~'if ~(:code pred) ~(:code then) ~(:code else))
             cast (list cast))
     :return-type return-type}))

(defmulti codegen-call
  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (name f)) arg-types))))

(defmethod codegen-expr :call [{:keys [args] :as expr} _]
  (codegen-call (-> expr
                    (assoc :emitted-args (mapv :code args)
                           :arg-types (mapv :return-type args)))))

(defmethod codegen-call [:= Number Number] [{:keys [emitted-args]}]
  {:code `(== ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:= Object Object] [{:keys [emitted-args]}]
  {:code `(= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:!= Object Object] [{:keys [emitted-args]}]
  {:code `(not= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:< Number Number] [{:keys [emitted-args]}]
  {:code `(< ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:< Date Date] [{:keys [emitted-args]}]
  {:code `(< ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:< Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(neg? (compare ~@emitted-args))
   :return-type Boolean})

(defmethod codegen-call [:< types/byte-array-class types/byte-array-class] [{:keys [emitted-args]}]
  {:code `(neg? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type Boolean})

(defmethod codegen-call [:< String String] [{:keys [emitted-args]}]
  {:code `(neg? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type Boolean})

(prefer-method codegen-call [:< Number Number] [:< Comparable Comparable])
(prefer-method codegen-call [:< Date Date] [:< Comparable Comparable])
(prefer-method codegen-call [:< String String] [:< Comparable Comparable])

(defmethod codegen-call [:<= Number Number] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Date Date] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:<= types/byte-array-class types/byte-array-class] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:<= String String] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type Boolean})

(prefer-method codegen-call [:<= Number Number] [:<= Comparable Comparable])
(prefer-method codegen-call [:<= Date Date] [:<= Comparable Comparable])
(prefer-method codegen-call [:<= String String] [:<= Comparable Comparable])

(defmethod codegen-call [:> Number Number] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Date Date] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(pos? (compare ~@emitted-args))
   :return-type Boolean})

(defmethod codegen-call [:> types/byte-array-class types/byte-array-class] [{:keys [emitted-args]}]
  {:code `(pos? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type Boolean})

(defmethod codegen-call [:> String String] [{:keys [emitted-args]}]
  {:code `(pos? (compare-nio-buffers-unsigned ~@emitted-args))
   :return-type Boolean})

(prefer-method codegen-call [:> Number Number] [:> Comparable Comparable])
(prefer-method codegen-call [:> Date Date] [:> Comparable Comparable])
(prefer-method codegen-call [:> String String] [:> Comparable Comparable])

(defmethod codegen-call [:>= Number Number] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Date Date] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:>= types/byte-array-class types/byte-array-class] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:>= String String] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare-nio-buffers-unsigned ~@emitted-args)))
   :return-type Boolean})

(prefer-method codegen-call [:>= Number Number] [:>= Comparable Comparable])
(prefer-method codegen-call [:>= Date Date] [:>= Comparable Comparable])
(prefer-method codegen-call [:>= String String] [:>= Comparable Comparable])

(defmethod codegen-call [:and Boolean Boolean] [{:keys [emitted-args]}]
  {:code `(and ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:or Boolean Boolean] [{:keys [emitted-args]}]
  {:code `(or ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:not Boolean] [{:keys [emitted-args]}]
  {:code `(not ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:+ Number Number] [{:keys [emitted-args], [x-type y-type] :arg-types}]
  {:code `(+ ~@emitted-args)
   :return-type (widen-numeric-types x-type y-type)})

(defmethod codegen-call [:- Number Number] [{:keys [emitted-args], [x-type y-type] :arg-types}]
  {:code `(- ~@emitted-args)
   :return-type (widen-numeric-types x-type y-type)})

(defmethod codegen-call [:- Number] [{:keys [emitted-args], [x-type] :arg-types}]
  {:code `(- ~@emitted-args)
   :return-type x-type})

(defmethod codegen-call [:* Number Number] [{:keys [emitted-args], [x-type y-type] :arg-types}]
  {:code `(* ~@emitted-args)
   :return-type (widen-numeric-types x-type y-type)})

(defmethod codegen-call [:% Number Number] [{:keys [emitted-args], [x-type y-type] :arg-types}]
  {:code `(mod ~@emitted-args)
   :return-type (widen-numeric-types x-type y-type)})

(defmethod codegen-call [:/ Number Number] [{:keys [emitted-args], [x-type y-type] :arg-types}]
  {:code `(/ ~@emitted-args)
   :return-type (widen-numeric-types x-type y-type)})

(defmethod codegen-call [:/ Long Long] [{:keys [emitted-args]}]
  {:code `(quot ~@emitted-args)
   :return-type Long})

(defmethod codegen-call [:like Comparable String] [{[{x :code} {pattern :code}] :args}]
  {:code `(boolean (re-find ~(re-pattern (str "^" (str/replace (resolve-string pattern) #"%" ".*") "$"))
                            (resolve-string ~x)))
   :return-type Boolean})

(defmethod codegen-call [:substr Comparable Long Long] [{[{x :code} {start :code} {length :code}] :args}]
  {:code `(ByteBuffer/wrap (.getBytes (subs (resolve-string ~x) (dec ~start) (+ (dec ~start) ~length))
                                      StandardCharsets/UTF_8))
   :return-type String})

(defmethod codegen-call [:extract String Date] [{[{field :code} {x :code}] :args}]
  {:code `(.get (.atOffset (Instant/ofEpochMilli ~x) ZoneOffset/UTC)
                ~(case (resolve-string field)
                   "YEAR" `ChronoField/YEAR
                   "MONTH" `ChronoField/MONTH_OF_YEAR
                   "DAY" `ChronoField/DAY_OF_MONTH
                   "HOUR" `ChronoField/HOUR_OF_DAY
                   "MINUTE" `ChronoField/MINUTE_OF_HOUR))
   :return-type Long})

(doseq [^Method method (.getDeclaredMethods Math)
        :let [math-op (.getName method)
              boxed-types (map type->boxed-type (.getParameterTypes method))
              boxed-return-type (get type->boxed-type (.getReturnType method))]
        :when (and boxed-return-type (every? some? boxed-types))]
  (defmethod codegen-call (vec (cons (keyword math-op) boxed-types)) [{:keys [emitted-args]}]
    {:code `(~(symbol "Math" math-op) ~@emitted-args)
     :return-type boxed-return-type}))

(defn normalize-union-value [v]
  (cond
    (instance? Date v)
    (.getTime ^Date v)
    (string? v)
    (ByteBuffer/wrap (.getBytes ^String v StandardCharsets/UTF_8))
    (bytes? v)
    (ByteBuffer/wrap v)
    :else
    v))

;; TODO: cannot easily extend postwalk-expr, as metadata-var-lit-call
;; assumes the literal to been untouched.
(defn init-expressions [expr]
  (let [acc (atom [])]
    (w/postwalk (fn [expr]
                  (when (map? expr)
                    (let [{:keys [op init-variable init-code]} expr]
                      (when (and (= :literal op) init-variable)
                        (swap! acc concat [init-variable init-code]))))
                  expr)
                expr)
    @acc))

(defn- generate-code [arrow-types expr expression-type]
  (let [vars (variables expr)
        var->type (zipmap vars (map #(get types/arrow-type->java-type % Comparable) arrow-types))
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % {:var->type var->type}) expr)
        args (for [[k v] (map vector vars arrow-types)]
               (-> k (with-tag (get types/arrow-type->vector-type v DenseUnionVector))))]
    (case expression-type
      ::project
      (if (= Comparable return-type)
        `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
           (let [~@(init-expressions expr)]
             (dotimes [~idx-sym row-count#]
               (let [value# ~code
                     type-id# (types/arrow-type->type-id (types/->arrow-type (class value#)))
                     offset# (DenseUnionUtil/writeTypeId acc# ~idx-sym type-id#)]
                 (types/set-safe! (.getVectorByType acc# type-id#) offset# value#))))
           acc#)
        (let [arrow-return-type (types/->arrow-type return-type)
              ^Class vector-return-type (get types/arrow-type->vector-type arrow-return-type)
              return-type-id (types/arrow-type->type-id arrow-return-type)
              inner-acc-sym (-> (gensym "inner-acc") (with-tag vector-return-type))
              offset-sym (gensym "offset")]
          `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
             (let [~@(init-expressions expr)
                   ~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
               (dotimes [~idx-sym row-count#]
                 (let [~offset-sym (DenseUnionUtil/writeTypeId acc# ~idx-sym ~return-type-id)]
                   ~(if (.isAssignableFrom BaseVariableWidthVector vector-return-type)
                      `(let [bb# ~code]
                         (.set ~inner-acc-sym ~offset-sym bb# (.position bb#) (.remaining bb#)))
                      `(.set ~inner-acc-sym ~offset-sym ~(if (= BitVector vector-return-type)
                                                           `(if ~code 1 0)
                                                           code)))))
               acc#))))

      ::select
      (do (assert (= Boolean return-type))
          `(fn [[~@args] ^RoaringBitmap acc# ^long row-count#]
             (let [~@(init-expressions expr)]
               (dotimes [~idx-sym row-count#]
                 (try
                   (when ~code
                     (.add acc# ~idx-sym))
                   (catch ClassCastException e#))))
             acc#)))))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn- expression-in-vectors [^VectorSchemaRoot in expr]
  (vec (for [var (variables expr)]
         (util/maybe-single-child-dense-union (.getVector in (name var))))))

(defn- vector->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [^ValueVector v]
  (.getType (.getFieldType (.getField v))))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name expr]
  (reify ProjectionSpec
    (getField [_ _in-schema]
      (types/->primitive-dense-union-field col-name))

    (project [_ in-root out-vec]
      (let [in-vecs (expression-in-vectors in-root expr)
            arrow-types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code arrow-types expr ::project)
            expr-fn (memo-eval expr-code)
            ^DenseUnionVector out-vec out-vec]
        (expr-fn in-vecs out-vec (.getRowCount in-root))))))

(defn ->expression-root-selector ^core2.select.IVectorSchemaRootSelector [expr]
  (reify IVectorSchemaRootSelector
    (select [_ in]
      (let [in-vecs (expression-in-vectors in expr)
            arrow-types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code arrow-types expr ::select)
            expr-fn (memo-eval expr-code)
            acc (RoaringBitmap.)]
        (expr-fn in-vecs acc (.getRowCount in))))))

(defn ->expression-vector-selector ^core2.select.IVectorSelector [expr]
  (assert (= 1 (count (variables expr))))
  (reify IVectorSelector
    (select [_ v]
      (let [in-vecs [(util/maybe-single-child-dense-union v)]
            arrow-types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code arrow-types expr ::select)
            expr-fn (memo-eval expr-code)
            acc (RoaringBitmap.)]
        (expr-fn in-vecs acc (.getValueCount v))))))
