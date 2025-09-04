(ns xtdb.test-generators
  (:require [clojure.string :as str]
            [clojure.test.check.generators :as gen]
            [xtdb.time :as time])
  (:import [java.math BigDecimal]
           [java.net URI]
           [java.nio ByteBuffer]
           [java.time.temporal Temporal]
           [java.util List Map Set]
           [org.apache.arrow.memory BufferAllocator]
           [xtdb Types]
           [xtdb.arrow Vector]))

;; Simple types
;; TODO: Ensure all arrow types are covered here
(def nil-gen (gen/return nil))
(def bool-gen gen/boolean)
(def i8-gen gen/byte)
(def i16-gen (gen/let [v (gen/choose Short/MIN_VALUE Short/MAX_VALUE)] (short v)))
(def i32-gen (gen/let [v (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE)] (int v)))
(def i64-gen (gen/choose Long/MIN_VALUE Long/MAX_VALUE))
(def f64-gen gen/double)
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

(def value-gen
  (gen/frequency
   [[8 simple-gen]
    [2 (list-gen simple-gen)]
    [2 (struct-gen simple-gen)]
    [1 (set-gen simple-gen)]
    [1 (union-gen simple-gen)]]))

(def recursive-value-gen
  (gen/recursive-gen
   (fn [inner-gen]
     (gen/frequency
      [[8 simple-gen]
       [2 (list-gen inner-gen)]
       [2 (struct-gen inner-gen)]
       [1 (set-gen inner-gen)]
       [1 (union-gen inner-gen simple-gen)]]))
   simple-gen))

(def field-type-gen
  (gen/let [v recursive-value-gen]
    (Types/toFieldType v)))

;; TODO: Generating simple keys here for now, trying to ensure some overlap between records
(defn generate-record
  ([]
   (generate-record {}))
  ([{:keys [potential-doc-ids]}]
   (gen/let [id (if potential-doc-ids
                  (gen/elements potential-doc-ids)
                  (gen/one-of [i64-gen safe-keyword-gen uuid-gen]))
             num-fields (gen/choose 1 5)
             field-keys (gen/vector-distinct
                         (gen/elements [:a :b :c :d :e :f :g :h :i :j])
                         {:num-elements num-fields
                          :max-tries 100})
             field-values (gen/vector recursive-value-gen num-fields)]
     (-> (zipmap field-keys field-values)
         (assoc :xt/id id)))))

(defn typed-vector-vs-gen
  [element-gen min-length max-length]
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
  (typed-vector-vs-gen recursive-value-gen min-length max-length))

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
