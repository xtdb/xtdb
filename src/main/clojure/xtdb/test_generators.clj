(ns xtdb.test-generators
  (:require [clojure.string :as str]
            [clojure.test.check.generators :as gen])
  (:import [java.math BigDecimal]
           [java.net URI]
           [java.nio ByteBuffer]))

;; Simple types
;; TODO: Ensure all arrow types are covered here
(def nil-gen (gen/return nil))
(def bool-gen gen/boolean)
(def i8-gen gen/byte)
(def i16-gen (gen/let [v (gen/choose Short/MIN_VALUE Short/MAX_VALUE)] (short v)))
(def i32-gen (gen/let [v (gen/choose Integer/MAX_VALUE Integer/MAX_VALUE)] (int v)))
(def i64-gen (gen/choose Long/MIN_VALUE Long/MAX_VALUE))
(def f64-gen gen/double)
(def decimal-gen
  (gen/let [^double v (gen/double* {:infinite? false :NaN? false})]
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

(def simple-gen
  (gen/one-of [nil-gen bool-gen
               i8-gen i16-gen i32-gen i64-gen
               f64-gen decimal-gen
               utf8-gen varbinary-gen
               keyword-gen uuid-gen uri-gen
               instant-gen local-date-gen local-time-gen
               local-datetime-gen offset-datetime-gen zoned-datetime-gen
               duration-gen interval-gen]))

;; Composite Types
(def list-gen
  (fn [element-gen] (gen/vector element-gen 1 10)))

(def safe-keyword-gen
  (gen/let [s (gen/such-that #(not (str/blank? %)) gen/string-alphanumeric 100)]
    (keyword (str/lower-case s))))

(def struct-gen
  (fn [value-gen]
    (gen/let [num-keys (gen/choose 1 10)
              keys (gen/vector-distinct-by #(str/lower-case (name %)) safe-keyword-gen {:num-elements num-keys :max-tries 100})
              values (gen/vector value-gen num-keys)]
      (->> (zipmap keys values)
           (filter val)
           (into {})))))

(def set-gen
  (fn [element-gen] (gen/set element-gen {:min-elements 0 :max-elements 10})))

(def union-gen
  (fn [& generators] (gen/one-of generators)))

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
