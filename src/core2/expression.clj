(ns core2.expression
  (:require [clojure.string :as str]
            core2.operator.project
            core2.operator.select
            [core2.relation :as rel]
            [core2.types :as types]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.operator.project.ProjectionSpec
           [core2.operator.select IColumnSelector IRelationSelector]
           [core2.relation IReadColumn IReadRelation]
           java.lang.reflect.Method
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Duration Instant ZoneOffset]
           [java.time.temporal ChronoField ChronoUnit]
           [java.util Date LinkedHashMap]
           org.apache.arrow.vector.types.Types$MinorType
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
    (symbol? form) (if (.startsWith (name form) "?")
                     {:op :param, :param form}
                     {:op :variable, :variable form})

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

    :else {:op :literal, :literal form}))

(defmulti direct-child-exprs
  (fn [{:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod direct-child-exprs ::default [_] #{})
(defmethod direct-child-exprs :if [{:keys [pred then else]}] [pred then else])
(defmethod direct-child-exprs :call [{:keys [args]}] args)

(defmulti postwalk-expr
  (fn [f {:keys [op] :as expr}]
    op)
  :default ::default)

(defmethod postwalk-expr ::default [f expr]
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

(defn lits->params [expr]
  (->> expr
       (postwalk-expr (fn [{:keys [op] :as expr}]
                        (case op
                          :literal (let [{:keys [literal]} expr
                                         sym (gensym 'literal)]
                                     (-> {:op :param, :param sym, :literal literal}
                                         (vary-meta (fnil into {})
                                                    {:literals {sym literal}
                                                     :params #{sym}})))

                          :param (-> expr
                                     (vary-meta (fnil into {})
                                                {:params #{(:param expr)}}))

                          (let [child-exprs (direct-child-exprs expr)]
                            (-> expr
                                (vary-meta (fnil into {})
                                           {:literals (->> child-exprs
                                                           (into {} (mapcat (comp :literals meta))))
                                            :params (->> child-exprs
                                                         (into #{} (mapcat (comp :params meta))))}))))))))

(defn expr-seq [expr]
  (lazy-seq
   (cons expr (mapcat expr-seq (direct-child-exprs expr)))))

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(defn variables [expr]
  (->> (expr-seq expr)
       (into [] (comp (filter (comp #(= :variable %) :op))
                      (map :variable)
                      (distinct)))))

(def type->cast
  {Long 'long
   Double 'double
   Date 'long
   Duration 'long
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
  (fn [{:keys [op]} {:keys [var->types]}]
    op))

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x))

(defmethod codegen-expr :param [{:keys [param] :as expr} {:keys [param->type]}]
  (let [return-type (get types/arrow-type->java-type
                         (or (get param->type param)
                             (throw (IllegalArgumentException. (str "parameter not provided: " param))))
                         Comparable)]
    (into {:code param
           :return-type return-type}
          (select-keys expr #{:literal}))))

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

(defmethod codegen-expr :variable [{:keys [variable]} {:keys [var->types]}]
  (let [var-types (or (get var->types variable)
                      (throw (IllegalArgumentException. (str "unknown variable: " variable))))
        var-type (or (when (= 1 (count var-types))
                       (get types/minor-type->java-type (first var-types)))
                     Comparable)]
    {:code (condp = var-type
             Boolean `(.getBool ~variable ~idx-sym)
             Long `(.getLong ~variable ~idx-sym)
             Double `(.getDouble ~variable ~idx-sym)
             String `(.getBuffer ~variable ~idx-sym)
             types/byte-array-class `(.getBuffer ~variable ~idx-sym)
             Date `(.getDate ~variable ~idx-sym)
             Duration `(.getDuration ~variable ~idx-sym)
             `(normalize-union-value (.getObject ~variable ~idx-sym)))
     :return-type var-type}))

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

(defmethod codegen-call [:< Duration Duration] [{:keys [emitted-args]}]
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
(prefer-method codegen-call [:< Duration Duration] [:< Comparable Comparable])
(prefer-method codegen-call [:< String String] [:< Comparable Comparable])

(defmethod codegen-call [:<= Number Number] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Date Date] [{:keys [emitted-args]}]
  {:code `(<= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:<= Duration Duration] [{:keys [emitted-args]}]
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
(prefer-method codegen-call [:<= Duration Duration] [:<= Comparable Comparable])
(prefer-method codegen-call [:<= String String] [:<= Comparable Comparable])

(defmethod codegen-call [:> Number Number] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Date Date] [{:keys [emitted-args]}]
  {:code `(> ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:> Duration Duration] [{:keys [emitted-args]}]
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
(prefer-method codegen-call [:> Duration Duration] [:> Comparable Comparable])
(prefer-method codegen-call [:> String String] [:> Comparable Comparable])

(defmethod codegen-call [:>= Number Number] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Date Date] [{:keys [emitted-args]}]
  {:code `(>= ~@emitted-args)
   :return-type Boolean})

(defmethod codegen-call [:>= Duration Duration] [{:keys [emitted-args]}]
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
(prefer-method codegen-call [:>= Duration Duration] [:>= Comparable Comparable])
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

(defmethod codegen-call [:like Comparable String] [{[{x :code} {:keys [literal]}] :args}]
  {:code `(boolean (re-find ~(re-pattern (str "^" (str/replace literal #"%" ".*") "$"))
                            (resolve-string ~x)))
   :return-type Boolean})

(defmethod codegen-call [:substr Comparable Long Long] [{[{x :code} {start :code} {length :code}] :args}]
  {:code `(ByteBuffer/wrap (.getBytes (subs (resolve-string ~x) (dec ~start) (+ (dec ~start) ~length))
                                      StandardCharsets/UTF_8))
   :return-type String})

(defmethod codegen-call [:extract String Date] [{[{field :literal} {x :code}] :args}]
  {:code `(.get (.atOffset (Instant/ofEpochMilli ~x) ZoneOffset/UTC)
                ~(case field
                   "YEAR" `ChronoField/YEAR
                   "MONTH" `ChronoField/MONTH_OF_YEAR
                   "DAY" `ChronoField/DAY_OF_MONTH
                   "HOUR" `ChronoField/HOUR_OF_DAY
                   "MINUTE" `ChronoField/MINUTE_OF_HOUR))
   :return-type Long})

(defmethod codegen-call [:date-trunc String Date] [{[{field :literal} {x :code}] :args}]
  {:code `(Date/from (.truncatedTo (Instant/ofEpochMilli ~x)
                                   ~(case field
                                      "YEAR" `ChronoUnit/YEARS
                                      "MONTH" `ChronoUnit/MONTHS
                                      "DAY" `ChronoUnit/DAYS
                                      "HOUR" `ChronoUnit/HOURS
                                      "MINUTE" `ChronoUnit/MINUTES
                                      "SECOND" `ChronoUnit/SECONDS)))
   :return-type Date})

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
    (instance? Duration v)
    (.toMillis ^Duration v)
    (string? v)
    (ByteBuffer/wrap (.getBytes ^String v StandardCharsets/UTF_8))
    (bytes? v)
    (ByteBuffer/wrap v)
    :else
    v))

(defn normalise-params [expr params]
  (let [expr (lits->params expr)
        {expr-params :params, lits :literals} (meta expr)
        params (->> expr-params
                    (util/into-linked-map
                     (map (fn [param-k]
                            (MapEntry/create param-k
                                             (val (or (find params param-k)
                                                      (find lits param-k))))))))]
    {:expr expr
     :params params
     :param-types (->> params
                       (util/into-linked-map
                        (util/map-entries (fn [param-k param-v]
                                            (let [arrow-type (types/->arrow-type (class param-v))
                                                  normalized-expr-type (class (normalize-union-value param-v))
                                                  primitive-tag (get type->cast normalized-expr-type)]
                                              (MapEntry/create (cond-> param-k
                                                                 primitive-tag (with-tag primitive-tag))
                                                               arrow-type))))))

     :emitted-params (->> params
                          (util/into-linked-map
                           (util/map-values (fn [_param-k param-v]
                                              (normalize-union-value param-v)))))}))

(defn- expression-in-cols [^IReadRelation in-rel expr]
  (->> (variables expr)
       (util/into-linked-map
        (map (fn [variable]
               (MapEntry/create variable (.readColumn in-rel (name variable))))))))

(def ^:private return-type->append-sym
  {Boolean '.appendBool
   Long '.appendLong
   Double '.appendDouble
   String '.appendStringBuffer
   types/byte-array-class '.appendBuffer
   Date '.appendDate
   Duration '.appendDuration})

(def ^:private return-type->minor-type-sym
  {Boolean `Types$MinorType/BIT
   Long `Types$MinorType/BIGINT
   Double `Types$MinorType/FLOAT8
   String `Types$MinorType/VARCHAR
   types/byte-array-class `Types$MinorType/VARBINARY
   Date `Types$MinorType/TIMESTAMPMILLI
   Duration `Types$MinorType/DURATION})

(defn- generate-projection [col-name expr var-types param-types]
  (let [codegen-opts {:var->types var-types, :param->type param-types}
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % codegen-opts) expr)
        variables (->> (keys var-types)
                       (map #(with-tag % IReadColumn)))

        allocator-sym (gensym 'allocator)
        out-col-sym (gensym 'out-vec)]

    `(fn [~allocator-sym [~@variables] [~@(keys param-types)] ^long row-count#]
       (let [~out-col-sym ~(if-let [minor-type-sym (return-type->minor-type-sym return-type)]
                             `(rel/->vector-append-column ~allocator-sym ~col-name ~minor-type-sym)
                             `(rel/->fresh-append-column ~allocator-sym ~col-name))]
         (dotimes [~idx-sym row-count#]
           (~(get return-type->append-sym return-type '.appendObject)
            ~out-col-sym
            ~code))
         (.read ~out-col-sym)))))

(def ^:private memo-generate-projection (memoize generate-projection))
(def ^:private memo-eval (memoize eval))

(defn ->expression-projection-spec
  (^core2.operator.project.ProjectionSpec [col-name expr]
   (->expression-projection-spec col-name expr {}))

  (^core2.operator.project.ProjectionSpec [col-name expr params]
   (let [{:keys [expr param-types emitted-params]} (normalise-params expr params)]
     (reify ProjectionSpec
       (project [_ allocator in-rel]
         (let [in-cols (expression-in-cols in-rel expr)
               var-types (->> in-cols
                              (util/into-linked-map
                               (util/map-values (fn [_variable ^IReadColumn read-col]
                                                  (.minorTypes read-col)))))
               expr-fn (-> (memo-generate-projection col-name expr var-types param-types)
                           (memo-eval))]
           (expr-fn allocator (vals in-cols) (vals emitted-params) (.rowCount in-rel))))))))

(defn- generate-selection [expr var-types param-types]
  (let [codegen-opts {:var->types var-types, :param->type param-types}
        {:keys [code return-type]} (postwalk-expr #(codegen-expr % codegen-opts) expr)
        variables (->> (keys var-types)
                       (map #(with-tag % IReadColumn)))]

    (assert (= Boolean return-type))
    `(fn [[~@variables] [~@(keys param-types)] ^long row-count#]
       (let [acc# (RoaringBitmap.)]
         (dotimes [~idx-sym row-count#]
           (try
             (when ~code
               (.add acc# ~idx-sym))
             (catch ClassCastException e#)))
         acc#))))

(def ^:private memo-generate-selection (memoize generate-selection))

(defn ->expression-relation-selector
  (^core2.operator.select.IRelationSelector [expr]
   (->expression-relation-selector expr {}))

  (^core2.operator.select.IRelationSelector [expr params]
   (let [{:keys [expr param-types emitted-params]} (normalise-params expr params)]
     (reify IRelationSelector
       (select [_ in]
         (let [in-cols (expression-in-cols in expr)
               var-types (->> in-cols
                              (util/into-linked-map
                               (util/map-values (fn [_variable ^IReadColumn read-col]
                                                  (.minorTypes read-col)))))
               expr-code (memo-generate-selection expr var-types param-types)
               expr-fn (memo-eval expr-code)]
           (expr-fn (vals in-cols) (vals emitted-params) (.rowCount in))))))))

(defn ->expression-column-selector
  (^core2.operator.select.IColumnSelector [expr]
   (->expression-column-selector expr {}))

  (^core2.operator.select.IColumnSelector [expr params]
   (let [{:keys [expr param-types emitted-params]} (normalise-params expr params)
         vars (variables expr)
         _ (assert (= 1 (count vars)))
         variable (first vars)]
     (reify IColumnSelector
       (select [_ in-col]
         (let [in-cols (doto (LinkedHashMap.)
                         (.put variable in-col))
               var-types (doto (LinkedHashMap.)
                           (.put variable (.minorTypes in-col)))
               expr-code (memo-generate-selection expr var-types param-types)
               expr-fn (memo-eval expr-code)]
           (expr-fn (vals in-cols) (vals emitted-params) (.valueCount in-col))))))))
