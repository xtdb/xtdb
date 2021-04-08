(ns core2.expression
  (:require core2.operator.project
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           [core2.select IVectorSchemaRootSelector IVectorSelector]
           java.lang.reflect.Method
           java.time.LocalDateTime
           java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector Float8Vector NullVector TimeStampMilliVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

;; TODO:

;; Normalise constants and get methods for
;; Text/bytes/Dates/Intervals. Support ArrowBufPointers?

;; Add tests for things beyond numbers.

;; Figure out how to use this for metadata.

;; Example of other built-in ops needed are things related to strings,
;; dates, casts and temporal intervals.

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
    (or (number? form)
        (boolean? form)
        (instance? Date form)
        (string? form))
    {:op :literal, :literal form}

    (symbol? form) {:op :variable, :variable form}

    (sequential? form) (let [[f & args] form]
                         (case f
                           'if (do
                                 (when-not (= 3 (count args))
                                   (throw (IllegalArgumentException. "'if' expects 3 args: " (pr-str form))))

                                 (let [[pred then else] args]
                                   {:op :if,
                                    :pred (form->expr pred),
                                    :then (form->expr then),
                                    :else (form->expr else)}))

                           (-> {:op :call, :f f, :args (mapv form->expr args)}
                               expand-variadics)))

    :else (throw (IllegalArgumentException. (str "unexpected form: " (pr-str form))))))

(defn postwalk-expr [f {:keys [op] :as expr}]
  (case op
    (:literal :variable) (f expr)
    :if (let [{:keys [pred then else]} expr]
          (f {:op :if
              :pred (postwalk-expr f pred)
              :then (postwalk-expr f then)
              :else (postwalk-expr f else)}))
    :call (let [{expr-f :f, :keys [args]} expr]
            (f {:op :call
                :f expr-f
                :args (mapv #(postwalk-expr f %) args)}))))

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

(def ^:private arrow-type->vector-type
  {(.getType Types$MinorType/NULL) NullVector
   (.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/VARBINARY) VarBinaryVector
   (.getType Types$MinorType/VARCHAR) VarCharVector
   (.getType Types$MinorType/TIMESTAMPMILLI) TimeStampMilliVector
   (.getType Types$MinorType/BIT) BitVector})

(def ^:private byte-array-class (Class/forName "[B"))

(def ^:private arrow-type->java-type
  {(.getType Types$MinorType/NULL) nil
   (.getType Types$MinorType/BIGINT) Long
   (.getType Types$MinorType/FLOAT8) Double
   (.getType Types$MinorType/VARBINARY) byte-array-class
   (.getType Types$MinorType/VARCHAR) String
   (.getType Types$MinorType/TIMESTAMPMILLI) Date
   (.getType Types$MinorType/BIT) Boolean})

(def ^:private type->cast
  {Long 'long
   Double 'double
   byte-array-class 'bytes
   String 'str
   Date 'long
   Boolean 'boolean})

(def ^:private type->boxed-type {Double/TYPE Double
                                 Long/TYPE Long
                                 Boolean/TYPE Boolean})

(def ^:private idx-sym (gensym "idx"))

(defn- widen-numeric-types [type-x type-y]
  (when (and (.isAssignableFrom Number type-x)
             (.isAssignableFrom Number type-y))
    (if (and (= type-x Long) (= type-y Long))
      Long
      Double)))

(defmulti codegen-expr
  (fn [{:keys [op]} var->type]
    op))

(defmethod codegen-expr :literal [{:keys [literal]} _]
  {:code (if (instance? Date literal)
           (.getTime ^Date literal)
           literal)
   :return-type (class literal)})

(defmethod codegen-expr :variable [{:keys [variable]} {:keys [var->type]}]
  (let [type (or (get var->type variable)
                 (throw (IllegalArgumentException. (str "unknown variable: " variable))))]
    {:code (condp = type
             Boolean `(== 1 (.get ~variable ~idx-sym))
             String `(str (.getObject ~variable ~idx-sym))
             Comparable `(normalize-union-value (.getObject ~variable ~idx-sym))
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

(defmethod codegen-call [:= byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(Arrays/equals ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:!= Object Object] [{:keys [emitted-args]}]
  {:code `(not= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:!= byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(not (Arrays/equals ~@emitted-args))
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

(defmethod codegen-call [:< byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(neg? (Arrays/compareUnsigned ~@emitted-args))
   :return-type Boolean})

(prefer-method codegen-call [:< Number Number] [:< Comparable Comparable])
(prefer-method codegen-call [:< Date Date] [:< Comparable Comparable])

(defmethod codegen-call [:<= Number Number] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Date Date] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(not (pos? (compare ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:<= byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(not (pos? (Arrays/compareUnsigned ~@emitted-args)))
   :return-type Boolean})

(prefer-method codegen-call [:<= Number Number] [:<= Comparable Comparable])
(prefer-method codegen-call [:<= Date Date] [:<= Comparable Comparable])

(defmethod codegen-call [:> Number Number] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Date Date] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(pos? (compare ~@emitted-args))
   :return-type Boolean})

(defmethod codegen-call [:> byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(pos? (Arrays/compareUnsigned ~@emitted-args))
   :return-type Boolean})

(prefer-method codegen-call [:> Number Number] [:> Comparable Comparable])
(prefer-method codegen-call [:> Date Date] [:> Comparable Comparable])

(defmethod codegen-call [:>= Number Number] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Date Date] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Comparable Comparable] [{:keys [emitted-args]}]
  {:code `(not (neg? (compare ~@emitted-args)))
   :return-type Boolean})

(defmethod codegen-call [:>= byte-array-class byte-array-class] [{:keys [emitted-args]}]
  {:code `(not (neg? (Arrays/compareUnsigned ~@emitted-args)))
   :return-type Boolean})

(prefer-method codegen-call [:>= Number Number] [:>= Comparable Comparable])
(prefer-method codegen-call [:>= Date Date] [:>= Comparable Comparable])

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
    (instance? LocalDateTime v)
    (.getTime (util/local-date-time->date v))
    (instance? Text v)
    (str v)
    :else
    v))

(defn- generate-code [arrow-types expr expression-type]
  (let [vars (variables expr)
        var->type (zipmap vars (map #(get arrow-type->java-type % Comparable) arrow-types))
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % {:var->type var->type}) expr)
        args (for [[k v] (map vector vars arrow-types)]
               (-> k (with-tag (get arrow-type->vector-type v DenseUnionVector))))]
    (case expression-type
      ::project
      (if (= Comparable return-type)
        `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
           (dotimes [~idx-sym row-count#]
             (let [value# ~code
                   type-id# (types/arrow-type->type-id (types/->arrow-type (class value#)))
                   offset# (util/write-type-id acc# ~idx-sym type-id#)]
               (types/set-safe! (.getVectorByType acc# type-id#) offset# value#)))
           acc#)
        (let [arrow-return-type (types/->arrow-type return-type)
              ^Class vector-return-type (get arrow-type->vector-type arrow-return-type)
              return-type-id (types/arrow-type->type-id arrow-return-type)
              inner-acc-sym (-> (gensym "inner-acc") (with-tag vector-return-type))]
          `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
             (let [~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
               (dotimes [~idx-sym row-count#]
                 (let [offset# (util/write-type-id acc# ~idx-sym ~return-type-id)]
                   (.set ~inner-acc-sym offset# ~(cond
                                                   (= BitVector vector-return-type)
                                                   `(if ~code 1 0)
                                                   (= VarCharVector vector-return-type)
                                                   `(Text. ~code)
                                                   :else
                                                   code))))
               acc#))))

      ::select
      (do (assert (= Boolean return-type))
          `(fn [[~@args] ^RoaringBitmap acc# ^long row-count#]
             (dotimes [~idx-sym row-count#]
               (try
                 (when ~code
                   (.add acc# ~idx-sym))
                 (catch ClassCastException e#)))
             acc#)))))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn- expression-in-vectors [^VectorSchemaRoot in expr]
  (vec (for [var (variables expr)]
         (util/maybe-single-child-dense-union (.getVector in (name var))))))

(defn- vector->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [^ValueVector v]
  (.getType (.getFieldType (.getField v))))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name form]
  (let [expr (form->expr form)]
    (reify ProjectionSpec
      (project [_ in allocator]
        (let [in-vecs (expression-in-vectors in expr)
              arrow-types (mapv vector->arrow-type in-vecs)
              expr-code (memo-generate-code arrow-types expr ::project)
              expr-fn (memo-eval expr-code)
              ^DenseUnionVector acc (.createVector (types/->primitive-dense-union-field col-name) allocator)]
          (expr-fn in-vecs acc (.getRowCount in)))))))

(defn ->expression-root-selector ^core2.select.IVectorSchemaRootSelector [form]
  (let [expr (form->expr form)]
    (reify IVectorSchemaRootSelector
      (select [_ in]
        (let [in-vecs (expression-in-vectors in expr)
              arrow-types (mapv vector->arrow-type in-vecs)
              expr-code (memo-generate-code arrow-types expr ::select)
              expr-fn (memo-eval expr-code)
              acc (RoaringBitmap.)]
          (expr-fn in-vecs acc (.getRowCount in)))))))

(defn ->expression-vector-selector ^core2.select.IVectorSelector [form]
  (let [expr (form->expr form)]
    (assert (= 1 (count (variables expr))))
    (reify IVectorSelector
      (select [_ v]
        (let [in-vecs [(util/maybe-single-child-dense-union v)]
              arrow-types (mapv vector->arrow-type in-vecs)
              expr-code (memo-generate-code arrow-types expr ::select)
              expr-fn (memo-eval expr-code)
              acc (RoaringBitmap.)]
          (expr-fn in-vecs acc (.getValueCount v)))))))

(defn- simplify-and-or-expr [{:keys [f args] :as expr}]
  (let [args (filterv some? args)]
    (case (count args)
      0 {:op :literal, :literal (case f and true, or false)}
      1 (first args)
      (-> expr (assoc :args args)))))

(defn- meta-fallback-expr [{:keys [op] :as expr}]
  (case op
    :literal nil
    :variable {:op :metadata-field-present, :field (:variable expr)}
    :if (let [{:keys [pred then else]} expr]
          {:op :if,
           :pred (meta-fallback-expr pred)
           :then (meta-fallback-expr then)
           :else (meta-fallback-expr else)})
    :call (let [{:keys [f args]} expr]
            (-> {:op :call
                 :f (if (= f 'or) 'or 'and)
                 :args (map meta-fallback-expr args)}
                simplify-and-or-expr))))

(declare meta-expr)

(defn call-meta-expr [{:keys [f args] :as expr}]
  (letfn [(var-lit-expr [f meta-value field literal]
            (simplify-and-or-expr
             {:op :call
              :f 'or
              :args (vec (for [field-type (cond
                                            (instance? Date literal) [Date]
                                            (number? literal) [Long Double]
                                            (string? literal) [String]
                                            (boolean? literal) [Boolean])]
                           {:op :metadata-var-lit-call,
                            :f f
                            :meta-value meta-value
                            :field-type field-type
                            :field field,
                            :literal literal}))}))

          (bool-expr [var-lit-f var-lit-meta-fn
                      lit-var-f lit-var-meta-fn]
            (let [[{x-op :op, :as x-arg} {y-op :op, :as y-arg}] args]
              (case [x-op y-op]
                [:literal :literal] expr
                [:variable :literal] (var-lit-expr var-lit-f var-lit-meta-fn
                                                   (:variable x-arg) (:literal y-arg))
                [:literal :variable] (var-lit-expr lit-var-f lit-var-meta-fn
                                                   (:variable y-arg) (:literal x-arg))
                nil)))]

    (or (case f
          and (-> {:op :call, :f 'and, :args (map meta-expr args)} simplify-and-or-expr)
          or (-> {:op :call, :f 'or, :args (map meta-expr args)} simplify-and-or-expr)
          < (bool-expr '< :min '> :max)
          <= (bool-expr '<= :min '>= :max)
          > (bool-expr '> :max '< :min)
          >= (bool-expr '>= :max '<= :min)
          = (meta-expr {:op :call,
                        :f 'and,
                        :args [{:op :call, :f '<=, :args args}
                               {:op :call, :f '>=, :args args}]})
          nil)

        (meta-fallback-expr expr))))

(defn- meta-expr [{:keys [op] :as expr}]
  (expand-variadics
   (case op
     (:literal :variable) nil
     :if {:op :call
          :f 'and
          :args [(meta-fallback-expr (:pred expr))
                 (-> {:op :call
                      :f 'or
                      :args [(meta-expr (:then expr))
                             (meta-expr (:else expr))]}
                     simplify-and-or-expr)]}
     :call (call-meta-expr expr))))
