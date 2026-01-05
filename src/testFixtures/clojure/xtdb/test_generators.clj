(ns xtdb.test-generators
  (:require [clojure.string :as str]
            [clojure.test.check.generators :as gen]
            [honey.sql :as sql]
            [xtdb.time :as time]
            [xtdb.types :as types])
  (:import [java.math BigDecimal]
           [java.net URI]
           [java.nio ByteBuffer]
           [java.time.temporal Temporal]
           [java.util List Map Set]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector.types.pojo Field]
           [xtdb.arrow ArrowTypes MergeTypes Vector VectorType]))

;; Simple types
;; TODO: Ensure all arrow types are covered here
(def nil-gen (gen/return nil))
(def bool-gen gen/boolean)
(def i8-gen gen/byte)
(def i16-gen (gen/let [v (gen/choose Short/MIN_VALUE Short/MAX_VALUE)] (short v)))
(def i32-gen (gen/let [v (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE)] (int v)))
(def i64-gen (gen/choose Long/MIN_VALUE Long/MAX_VALUE))
(def f64-gen (gen/double* {:infinite? false :NaN? false}))
(def decimal-gen (gen/let [^double v (gen/double* {:infinite? false :NaN? false})]
                   (BigDecimal/valueOf v)))

(def utf8-gen gen/string)
(def varbinary-gen (gen/let [bs gen/bytes] (ByteBuffer/wrap bs)))
(def keyword-gen (gen/one-of [gen/keyword gen/keyword-ns]))
(def uuid-gen gen/uuid)
(def uri-gen (gen/let [s gen/string-alphanumeric] (URI/create s)))

(def instant-gen
  (gen/fmap #(java.time.Instant/ofEpochSecond %)
            (gen/choose 0 2147483647)))

(def local-date-gen
  (gen/fmap java.time.LocalDate/ofEpochDay
            (gen/choose 0 50000)))

(def local-time-gen
  (gen/fmap java.time.LocalTime/ofSecondOfDay
            (gen/choose 0 86399)))

(def local-datetime-gen
  (gen/let [date local-date-gen
            time local-time-gen]
    (java.time.LocalDateTime/of date time)))

(def offset-datetime-gen
  (gen/let [ldt local-datetime-gen
            offset-hours (gen/choose -12 12)]
    (java.time.OffsetDateTime/of ldt (java.time.ZoneOffset/ofHours offset-hours))))

(def zoned-datetime-gen
  (gen/let [ldt local-datetime-gen]
    (java.time.ZonedDateTime/of ldt (java.time.ZoneId/of "UTC"))))

(def duration-gen (gen/return #xt/duration "PT1S"))
(def interval-gen (gen/return #xt/interval "P1YT1S"))

(def simple-type-gens
  [nil-gen bool-gen
   i8-gen i16-gen i32-gen i64-gen
   f64-gen decimal-gen
   utf8-gen varbinary-gen
   keyword-gen uuid-gen uri-gen
   instant-gen local-date-gen local-time-gen
   local-datetime-gen offset-datetime-gen zoned-datetime-gen
   duration-gen interval-gen])

(def simple-gen
  (gen/one-of simple-type-gens))

;; Composite Types
(defn list-gen [element-gen]
  (gen/vector element-gen 1 10))

(def non-blank-string-gen
  (gen/such-that #(not (str/blank? %)) gen/string-alphanumeric 100))

(def safe-keyword-gen
  (gen/let [s non-blank-string-gen]
    (keyword (str/lower-case s))))

(defn struct-gen [value-gen]
  (gen/let [num-keys (gen/choose 1 10)
            keys (gen/vector-distinct-by #(str/lower-case (name %)) safe-keyword-gen {:num-elements num-keys :max-tries 100})
            values (gen/vector value-gen num-keys)]
    (->> (zipmap keys values)
         (filter val)
         (into {}))))

(defn set-gen [element-gen]
  (gen/set element-gen {:min-elements 0 :max-elements 10}))

(defn union-gen [& generators]
  (gen/one-of generators))

(defn recursive-value-gen
  ([] (recursive-value-gen {:exclude-gens #{}}))
  ([{:keys [exclude-gens]}]
   (let [filtered-simple-type-gen (remove exclude-gens simple-type-gens)
         filtered-simple-gen (gen/one-of (vec filtered-simple-type-gen))
         exclude-set? (contains? exclude-gens set-gen)]
     (gen/recursive-gen
      (fn [inner-gen]
        (gen/frequency
          (remove nil?
           [[8 filtered-simple-gen]
            [2 (list-gen inner-gen)]
            [2 (struct-gen inner-gen)]
            (when-not exclude-set? [1 (set-gen inner-gen)])
            [1 (union-gen inner-gen filtered-simple-gen)]])))
      filtered-simple-gen))))

(defn distinct-value-gen
  ([length]
   (distinct-value-gen length {}))
  ([length {:keys [exclude-gens] :or {exclude-gens #{}}}]
   (gen/vector-distinct (recursive-value-gen {:exclude-gens exclude-gens}) {:num-elements length})))

(def field-type-gen
  (gen/let [v (recursive-value-gen)]
    (ArrowTypes/toFieldType v)))

;; TODO: Generating simple keys here for now, trying to ensure some overlap between records
(defn generate-record
  ([]
   (generate-record {}))
  ([{:keys [potential-doc-ids exclude-gens override-field-keys]
     :or {exclude-gens #{}}}]
   (gen/let [id (if potential-doc-ids
                  (gen/elements potential-doc-ids)
                  (gen/one-of [i64-gen safe-keyword-gen uuid-gen]))
             num-fields (if override-field-keys
                          (gen/return (count override-field-keys))
                          (gen/choose 1 5))
             field-keys (if override-field-keys
                          (gen/return override-field-keys)
                          (gen/vector-distinct
                           (gen/elements [:a :b :c :d :e :f :g :h :i :j])
                           {:num-elements num-fields
                            :max-tries 100}))
             field-values (gen/vector (recursive-value-gen {:exclude-gens exclude-gens}) num-fields)]
     (-> (zipmap field-keys field-values)
         (assoc :xt/id id)))))

(defn typed-vector-vs-gen [element-gen min-length max-length]
  (gen/let [vector-name non-blank-string-gen
            vs (gen/vector element-gen min-length max-length)]
    {:vec-name vector-name
     :vs vs
     :type element-gen}))

(defn single-type-vector-vs-gen
  [min-length max-length]
  (gen/let [type-gen (gen/elements simple-type-gens)]
    (typed-vector-vs-gen type-gen min-length max-length)))

(defn dense-union-vector-vs-gen
  [min-length max-length]
  (typed-vector-vs-gen (recursive-value-gen) min-length max-length))

(def vector-vs-gen
  (gen/frequency
   [[3 (single-type-vector-vs-gen 1 100)]
    [1 (dense-union-vector-vs-gen 1 100)]]))

(defn fixed-length-vector-vs-gen [length]
  (gen/frequency
   [[3 (single-type-vector-vs-gen length length)]
    [1 (dense-union-vector-vs-gen length length)]]))

(def two-distinct-single-type-vecs-gen
  (gen/let [vec1 (single-type-vector-vs-gen 1 100)
            vec2 (gen/such-that #(and (not= (:type %) (:type vec1))
                                      (not= (:vec-name %) (:vec-name vec1)))
                                (single-type-vector-vs-gen 1 100)
                                100)]
    [vec1 vec2]))

(def two-distinct-duvs-gen
  (gen/let [duv1 (dense-union-vector-vs-gen 1 100)
            duv2 (gen/such-that #(not= (:vec-name %) (:vec-name duv1))
                                (dense-union-vector-vs-gen 1 100)
                                100)]
    [duv1 duv2]))

(defn vec-gen->arrow-vec [^BufferAllocator allocator {:keys [^String vec-name ^List vs]}]
  (Vector/fromList allocator vec-name vs))

;; TODO - Any utils/other way we could handle this?
(defn normalize-for-comparison [obj]
  (cond
    (and (number? obj) (Double/isNaN (double obj)))
    ::nan

    (instance? ByteBuffer obj)
    (vec (.array ^ByteBuffer obj))

    (bytes? obj)
    (vec obj)

    (instance? Temporal obj)
    (try
      (time/->instant obj)
      (catch Exception _ obj))

    (instance? List obj)
    (vec (map normalize-for-comparison obj))

    (vector? obj)
    (mapv normalize-for-comparison obj)

    (list? obj)
    (map normalize-for-comparison obj)

    (or (instance? Set obj) (set? obj))
    (set (map normalize-for-comparison obj))

    (or (instance? Map obj) (map? obj))
    (into {} (map (fn [[k v]]
                    (let [norm-k (cond
                                   (keyword? k) (name k)
                                   :else (str k))]
                      [norm-k (normalize-for-comparison v)]))
                  obj))

    :else obj))

(defn lists-equal-normalized? [list1 list2]
  (= (map normalize-for-comparison list1)
     (map normalize-for-comparison list2)))

(defn vec-type->value-generator
  "Generate a value generator for a given Arrow Field"
  [^VectorType vec-type]
  (let [arrow-type (.getArrowType vec-type)
        nullable? (.isNullable vec-type)]
    (gen/frequency
     (cond-> [[15 (condp = arrow-type
                    #xt.arrow/type :i8 i8-gen
                    #xt.arrow/type :i16 i16-gen
                    #xt.arrow/type :i32 i32-gen
                    #xt.arrow/type :i64 i64-gen
                    #xt.arrow/type :f64 f64-gen
                    #xt.arrow/type [:decimal 32 1 128] decimal-gen
                    #xt.arrow/type :utf8 utf8-gen
                    #xt.arrow/type :varbinary varbinary-gen
                    #xt.arrow/type :keyword keyword-gen
                    #xt.arrow/type :uuid uuid-gen
                    #xt.arrow/type :uri uri-gen
                    #xt.arrow/type :bool bool-gen
                    #xt.arrow/type :instant instant-gen
                    #xt.arrow/type [:date :day] local-date-gen
                    #xt.arrow/type [:time-local :nano] local-time-gen
                    #xt.arrow/type :list (gen/vector (vec-type->value-generator (first (.getChildren vec-type))) 0 10)
                    #xt.arrow/type :set (gen/set (vec-type->value-generator (first (.getChildren vec-type))) {:min-elements 0 :max-elements 10})
                    #xt.arrow/type :union (gen/one-of (map vec-type->value-generator (.getChildren vec-type)))
                    #xt.arrow/type :struct (gen/let [entries (apply gen/tuple (map (fn [^Field child-field]
                                                                                     (gen/let [v (vec-type->value-generator child-field)]
                                                                                       (when v [(keyword (.getName child-field)) v])))
                                                                                   (.getChildren vec-type)))]
                                             (->> (filter some? entries)
                                                  (into {})))
                    ;; fallback for unknown types
                    simple-gen)]]
       nullable? (conj [1 (gen/return nil)])))))

(defn records->generator
  "Given a list of records, produce a generator that will generate similar records"
  [records]
  (vec-type->value-generator (MergeTypes/mergeTypes (mapv VectorType/fromValue records))))

(MergeTypes/mergeTypes (mapv VectorType/fromValue [{:a 1}]))

(defn info-schema->generator
  "Given an XTDB info schema for a specific table, produce a generator that will generate records"
  [info-schema]
  (let [fields (->> info-schema
                    (filter (fn [{:keys [column-name]}]
                              (not (#{"_system_from" "_system_to" "_valid_from" "_valid_to"} column-name))))
                    (mapv (fn [{:keys [column-name data-type]}]
                            (types/->field (read-string data-type) column-name))))]
    (vec-type->value-generator (apply types/->field "docs" #xt.arrow/type :struct false fields))))

(defn generate-unique-id-records
  ([num-records] (generate-unique-id-records num-records num-records))
  ([min-records max-records]
   (gen/let [records (gen/vector (generate-record) min-records max-records)
             record-ids (gen/vector-distinct
                         (gen/one-of [i64-gen safe-keyword-gen uuid-gen])
                         {:num-elements (count records)
                          :max-tries 100})]
     (mapv (fn [record record-id] (assoc record :xt/id record-id)) records record-ids))))

(defn blocks-counts+records []
  (gen/no-shrink
   (gen/let [blocks-to-generate (gen/choose 2 5)
             rows-per-block (gen/elements [20 40 60 80 100])
             records (generate-unique-id-records (* blocks-to-generate rows-per-block))]
     {:expected-block-count blocks-to-generate
      :total-doc-count (* blocks-to-generate rows-per-block)
      :rows-per-block rows-per-block
      :partitioned-records (partition 10 records)})))

(defn update-statement-gen
  ([id]
   (update-statement-gen id {}))
  ([id {:keys [exclude-gens override-field-keys]}]
   (gen/let [num-fields (if override-field-keys
                          (gen/return (count override-field-keys))
                          (gen/choose 1 5))
             field-keys (if override-field-keys
                          (gen/return override-field-keys)
                          (gen/vector-distinct
                           (gen/elements [:a :b :c :d :e :f :g :h :i :j])
                           {:num-elements num-fields
                            :max-tries 100}))
             field-values (gen/vector (recursive-value-gen {:exclude-gens exclude-gens}) num-fields)]
     (let [lifted-vals (mapv (fn [v] [:lift v]) field-values)
           [sql-string & params] (sql/format {:update :docs
                                              :set (zipmap field-keys lifted-vals)
                                              :where [:= :_id id]})]
       {:fields (zipmap field-keys field-values)
        :sql-statement [:sql sql-string (vec params)]}))))
