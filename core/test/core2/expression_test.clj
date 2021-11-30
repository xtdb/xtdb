(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.local-node :as node]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import [java.time Duration ZonedDateTime]
           [org.apache.arrow.vector BigIntVector BitVector DurationVector Float4Vector Float8Vector IntVector NullVector SmallIntVector TimeStampMicroTZVector TimeStampMilliTZVector TimeStampNanoTZVector TimeStampSecTZVector TimeStampVector ValueVector VarCharVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType$Duration ArrowType$Timestamp FieldType]
           org.apache.arrow.vector.types.TimeUnit))

(t/use-fixtures :each tu/with-allocator)

(defn ->data-vecs []
  [(tu/->mono-vec "a" ty/float8-type (map double (range 1000)))
   (tu/->mono-vec "b" ty/float8-type (map double (range 1000)))
   (tu/->mono-vec "d" ty/bigint-type (range 1000))
   (tu/->mono-vec "e" ty/varchar-type (map #(format "%04d" %) (range 1000)))])

(defn- open-rel ^core2.vector.IIndirectRelation [vecs]
  (iv/->indirect-rel (map iv/->direct-vec vecs)))

(t/deftest test-simple-projection
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(project [form]
              (with-open [project-col (.project (expr/->expression-projection-spec "c" form {})
                                                tu/*allocator* in-rel)]
                (tu/<-column project-col)))]

      (t/is (= (mapv (comp double +) (range 1000) (range 1000))
               (project '(+ a b))))

      (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
               (project '(- a (* 2.0 b)))))

      (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
               (project '[:+ a [:+ b 2]]))
            "support keyword and vectors")

      (t/is (= (mapv + (repeat 2) (range 1000))
               (project '(+ 2 d)))
            "mixing types")

      (t/is (= (repeat 1000 true)
               (project '(= a d)))
            "predicate")

      (t/is (= (mapv #(Math/sin ^double %) (range 1000))
               (project '(sin a)))
            "math")

      (t/is (= (interleave (map float (range)) (repeat 500 0))
               (project '(if (= 0 (% a 2)) (/ a 2) 0)))
            "if")

      (t/is (thrown? IllegalArgumentException (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(select-relation [form params]
              (-> (.select (expr/->expression-relation-selector form params)
                           in-rel)
                  (.getCardinality)))

            (select-column [form ^String col-name params]
              (-> (.select (expr/->expression-column-selector form params)
                           (.vectorForName in-rel col-name))
                  (.getCardinality)))]

      (t/testing "selector"
        (t/is (= 500 (select-relation '(>= a 500) {})))
        (t/is (= 500 (select-column '(>= a 500) "a" {})))
        (t/is (= 500 (select-column '(>= e "0500") "e" {}))))

      (t/testing "parameter"
        (t/is (= 500 (select-column '(>= a ?a) "a" {'?a 500})))
        (t/is (= 500 (select-column '(>= e ?e) "e" {'?e "0500"})))))))

(t/deftest can-extract-min-max-range-from-expression
  (let [μs-2018 (util/instant->micros (util/->instant #inst "2018"))
        μs-2019 (util/instant->micros (util/->instant #inst "2019"))]
    (letfn [(transpose [[mins maxs]]
              (->> (map vector mins maxs)
                   (zipmap [:tt-end :id :tt-start :row-id :vt-start :vt-end])
                   (into {} (remove (comp #{[Long/MIN_VALUE Long/MAX_VALUE]} val)))))]
      (t/is (= {:vt-start [Long/MIN_VALUE μs-2019]
                :vt-end [(inc μs-2019) Long/MAX_VALUE]}
               (transpose (expr.temp/->temporal-min-max-range
                           {"_valid-time-start" '(<= _vt-time-start #inst "2019")
                            "_valid-time-end" '(> _vt-time-end #inst "2019")}
                           {}))))

      (t/testing "symbol column name"
        (t/is (= {:vt-start [μs-2019 μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {'_valid-time-start '(= _vt-time-start #inst "2019")}
                             {})))))

      (t/testing "conjunction"
        (t/is (= {:vt-start [Long/MIN_VALUE μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_valid-time-start" '(and (<= _vt-time-start #inst "2019")
                                                        (<= _vt-time-start #inst "2020"))}
                             {})))))

      (t/testing "disjunction not supported"
        (t/is (= {}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_valid-time-start" '(or (= _vt-time-start #inst "2019")
                                                       (= _vt-time-start #inst "2020"))}
                             {})))))

      (t/testing "parameters"
        (t/is (= {:vt-start [μs-2018 Long/MAX_VALUE]
                  :vt-end [Long/MIN_VALUE (dec μs-2018)]
                  :tt-start [Long/MIN_VALUE μs-2019]
                  :tt-end [(inc μs-2019) Long/MAX_VALUE]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"_tx-time-start" '(>= ?tt _tx-time-start)
                              "_tx-time-end" '(< ?tt _tx-time-end)
                              "_valid-time-start" '(<= ?vt _vt-time-start)
                              "_valid-time-end" '(> ?vt _vt-time-end)}
                             {'?tt (util/->instant #inst "2019",) '?vt (util/->instant #inst "2018")}))))))))

(t/deftest test-date-trunc
  (with-open [node (node/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id :foo, :date (util/->instant #inst "2021-01-21T12:34:56Z")}]])
          db (snap/snapshot (tu/component node ::snap/snapshot-factory) tx)]
      (t/is (= [{:trunc (util/->zdt #inst "2021-01-21")}]
               (op/query-ra '[:project [{trunc (date-trunc "DAY" date)}]
                              [:scan [date]]]
                            db)))

      (t/is (= [{:trunc (util/->zdt #inst "2021-01-21T12:34")}]
               (op/query-ra '[:project [{trunc (date-trunc "MINUTE" date)}]
                              [:scan [date]]]
                            db)))

      (t/is (= [{:trunc (util/->zdt #inst "2021-01-21")}]
               (op/query-ra '[:select (> trunc #inst "2021")
                              [:project [{trunc (date-trunc "DAY" date)}]
                               [:scan [date]]]]
                            db)))

      (t/is (= [{:trunc (util/->zdt #inst "2021-01-21")}]
               (op/query-ra '[:project [{trunc (date-trunc "DAY" trunc)}]
                              [:project [{trunc (date-trunc "MINUTE" date)}]
                               [:scan [date]]]]
                            db))))))

(defn- run-projection [rel form]
  (with-open [out-ivec (.project (expr/->expression-projection-spec "out" form {})
                                 tu/*allocator*
                                 rel)]
    {:res (tu/<-column out-ivec)
     :vec-type (let [out-vec (.getVector out-ivec)]
                 (if (instance? DenseUnionVector out-vec)
                   (->> (seq out-vec) (into #{} (map class)))
                   (class out-vec)))
     :nullable? (.isNullable (.getField (.getVector out-ivec)))}))

(t/deftest test-variadics
  (letfn [(run-test [f x y z]
            (with-open [rel (open-rel [(tu/->mono-vec "x" ty/bigint-type [x])
                                       (tu/->mono-vec "y" ty/bigint-type [y])
                                       (tu/->mono-vec "z" ty/bigint-type [z])])]
              (-> (run-projection rel (list f 'x 'y 'z))
                  :res first)))]

    (t/is (= 6 (run-test '+ 1 2 3)))
    (t/is (= 1 (run-test '- 4 2 1)))
    (t/is (true? (run-test '< 1 2 4)))
    (t/is (false? (run-test '> 4 1 2)))))

(t/deftest can-return-string-multiple-times
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable ty/bigint-type) [1 2 3])])]
    (t/is (= {:res ["foo" "foo" "foo"]
              :vec-type VarCharVector
              :nullable? false}
             (run-projection rel "foo")))))

(t/deftest test-cond
  (letfn [(run-test [expr xs]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable ty/bigint-type) xs)])]
              (run-projection rel expr)))]

    (t/is (= {:res ["big" "small" "tiny" "tiny"]
              :vec-type VarCharVector
              :nullable? false}
             (run-test '(cond (> x 100) "big", (> x 10) "small", "tiny")
                       [500 50 5 nil])))

    (t/is (= {:res ["big" "small" nil nil]
              :vec-type VarCharVector
              :nullable? true}
             (run-test '(cond (> x 100) "big", (> x 10) "small")
                       [500 50 5 nil])))))

(t/deftest test-let
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable ty/bigint-type) [1 2 3 nil])])]
    (t/is (= {:res [6 9 12 nil]
              :vec-type BigIntVector
              :nullable? true}
             (run-projection rel '(let [y (* x 2)
                                        y (+ y 3)]
                                    (+ x y)))))))

(t/deftest test-coalesce
  (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType/nullable ty/varchar-type) ["x" nil nil])
                             (tu/->mono-vec "y" (FieldType/nullable ty/varchar-type) ["y" "y" nil])])]
    (t/is (= {:res ["x" "y" "default"]
              :vec-type VarCharVector
              ;; TODO shouldn't be nullable
              :nullable? true}
             (run-projection rel '(coalesce x y "default"))))))

(t/deftest test-mixing-numeric-types
  (letfn [(run-test [f x y]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (ty/value->leg-type x)) [x])
                                       (tu/->mono-vec "y" (.arrowType (ty/value->leg-type y)) [y])])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first)
                  (dissoc :nullable?))))]

    (t/is (= {:res 6, :vec-type IntVector}
             (run-test '+ (int 4) (int 2))))

    (t/is (= {:res 6, :vec-type BigIntVector}
             (run-test '+ (int 2) (long 4))))

    (t/is (= {:res 6, :vec-type SmallIntVector}
             (run-test '+ (short 2) (short 4))))

    (t/is (= {:res 6.5, :vec-type Float4Vector}
             (run-test '+ (byte 2) (float 4.5))))

    (t/is (= {:res 6.5, :vec-type Float4Vector}
             (run-test '+ (float 2) (float 4.5))))

    (t/is (= {:res 6.5, :vec-type Float8Vector}
             (run-test '+ (float 2) (double 4.5))))

    (t/is (= {:res 6.5, :vec-type Float8Vector}
             (run-test '+ (int 2) (double 4.5))))

    (t/is (= {:res -2, :vec-type IntVector}
             (run-test '- (short 2) (int 4))))

    (t/is (= {:res 8, :vec-type SmallIntVector}
             (run-test '* (byte 2) (short 4))))

    (t/is (= {:res 2, :vec-type SmallIntVector}
             (run-test '/ (short 4) (byte 2))))

    (t/is (= {:res 2.0, :vec-type Float4Vector}
             (run-test '/ (float 4) (int 2))))))

(t/deftest test-throws-on-overflow
  (letfn [(run-unary-test [f x]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (ty/value->leg-type x)) [x])])]
              (-> (run-projection rel (list f 'x))
                  (update :res first))))

          (run-binary-test [f x y]
            (with-open [rel (open-rel [(tu/->mono-vec "x" (.arrowType (ty/value->leg-type x)) [x])
                                       (tu/->mono-vec "y" (.arrowType (ty/value->leg-type y)) [y])])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]

    (t/is (thrown? ArithmeticException
                   (run-binary-test '+ (Integer/MAX_VALUE) (int 4))))

    (t/is (thrown? ArithmeticException
                   (run-binary-test '- (Integer/MIN_VALUE) (int 4))))

    (t/is (thrown? ArithmeticException
                   (run-unary-test '- (Integer/MIN_VALUE))))

    (t/is (thrown? ArithmeticException
                   (run-binary-test '* (Integer/MIN_VALUE) (int 2))))

    #_ ; TODO this one throws IAE because that's what clojure.lang.Numbers/shortCast throws
    ;; the others are thrown by java.lang.Math/*Exact, which throw ArithmeticException
    (t/is (thrown? ArithmeticException
                   (run-unary-test '- (Short/MIN_VALUE))))))

(t/deftest test-polymorphic-columns
  (t/is (= {:res [1.2 1 3.4]
            :vec-type #{Float8Vector BigIntVector}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1.2 1 3.4])
                                      (tu/->duv "y" [3.4 (float 8.25)])])]
             (run-projection rel 'x))))

  (t/is (= {:res [4.4 9.75]
            :vec-type #{Float4Vector Float8Vector}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1 1.5])
                                      (tu/->duv "y" [3.4 (float 8.25)])])]
             (run-projection rel '(+ x y)))))

  (t/is (= {:res [(float 4.4) nil nil nil]
            :vec-type #{NullVector Float4Vector Float8Vector}
            :nullable? false}
           (with-open [rel (open-rel [(tu/->duv "x" [1 12 nil nil])
                                      (tu/->duv "y" [(float 3.4) nil 4.8 nil])])]
             (run-projection rel '(+ x y))))))

(t/deftest test-ternary-booleans
  (t/is (= [{:res [true false nil false false false nil false nil]
             :vec-type BitVector, :nullable? true}
            {:res [true true true true false nil true nil nil]
             :vec-type BitVector, :nullable? true}]
           (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType. true ty/bool-type nil)
                                                     [true true true false false false nil nil nil])
                                      (tu/->duv "y" [true false nil true false nil true false nil])])]
             [(run-projection rel '(and x y))
              (run-projection rel '(or x y))])))

  (t/is (= [{:res [false true nil]
             :vec-type BitVector, :nullable? true}
            {:res [true false false]
             :vec-type BitVector, :nullable? false}
            {:res [false true false]
             :vec-type BitVector, :nullable? false}
            {:res [false false true]
             :vec-type BitVector, :nullable? false}]
           (with-open [rel (open-rel [(tu/->mono-vec "x" (FieldType. true ty/bool-type nil) [true false nil])])]
             [(run-projection rel '(not x))
              (run-projection rel '(true? x))
              (run-projection rel '(false? x))
              (run-projection rel '(nil? x))]))))

(t/deftest test-mixing-timestamp-types
  (letfn [(->ts-vec [col-name time-unit, ^long value]
            (doto ^TimeStampVector (.createVector (ty/->field col-name (ArrowType$Timestamp. time-unit "UTC") false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (->dur-vec [col-name ^TimeUnit time-unit, ^long value]
            (doto (DurationVector. (ty/->field col-name (ArrowType$Duration. time-unit) false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (test-projection [f-sym ->x-vec ->y-vec]
            (with-open [^ValueVector x-vec (->x-vec)
                        ^ValueVector y-vec (->y-vec)]
              (-> (run-projection (iv/->indirect-rel [(iv/->direct-vec x-vec)
                                                      (iv/->direct-vec y-vec)])
                                  (list f-sym 'x 'y))
                  (dissoc :nullable?))))]

    (t/testing "ts/dur"
      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:02:03Z")]
                :vec-type TimeStampSecTZVector}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/SECOND 123))))

      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:00:00.123Z")]
                :vec-type TimeStampMilliTZVector}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 123))))

      (t/is (= {:res [(ZonedDateTime/parse "1970-01-01T00:02:34.000001234Z[UTC]")]
                :vec-type TimeStampNanoTZVector}
               (test-projection '+
                                #(->dur-vec "x" TimeUnit/SECOND 154)
                                #(->ts-vec "y" TimeUnit/NANOSECOND 1234))))

      (t/is (thrown? ArithmeticException
                     (test-projection '+
                                      #(->ts-vec "x" TimeUnit/MILLISECOND (- Long/MAX_VALUE 500))
                                      #(->dur-vec "y" TimeUnit/SECOND 1))))

      (t/is (= {:res [(util/->zdt #inst "2020-12-31T23:59:59.998Z")]
                :vec-type TimeStampMicroTZVector}
               (test-projection '-
                                #(->ts-vec "x" TimeUnit/MICROSECOND (util/instant->micros (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 2)))))

    (t/is (t/is (= {:res [(Duration/parse "PT23H59M59.999S")]
                    :vec-type DurationVector}
                   (test-projection '-
                                    #(->ts-vec "x" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-02")))
                                    #(->ts-vec "y" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-01T00:00:00.001Z")))))))

    (t/testing "durations"
      (letfn [(->bigint-vec [^String col-name, ^long value]
                (tu/->mono-vec col-name ty/bigint-type [value]))

              (->float8-vec [^String col-name, ^double value]
                (tu/->mono-vec col-name ty/float8-type [value]))]

        (t/is (= {:res [(Duration/parse "PT0.002001S")]
                  :vec-type DurationVector}
                 (test-projection '+
                                  #(->dur-vec "x" TimeUnit/MICROSECOND 1)
                                  #(->dur-vec "y" TimeUnit/MILLISECOND 2))))

        (t/is (= {:res [(Duration/parse "PT-1.999S")]
                  :vec-type DurationVector}
                 (test-projection '-
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->dur-vec "y" TimeUnit/SECOND 2))))

        (t/is (= {:res [(Duration/parse "PT0.002S")]
                  :vec-type DurationVector}
                 (test-projection '*
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->bigint-vec "y" 2))))

        (t/is (= {:res [(Duration/parse "PT10S")]
                  :vec-type DurationVector}
                 (test-projection '*
                                  #(->bigint-vec "x" 2)
                                  #(->dur-vec "y" TimeUnit/SECOND 5))))

        (t/is (= {:res [(Duration/parse "PT0.000012S")]
                  :vec-type DurationVector}
                 (test-projection '*
                                  #(->float8-vec "x" 2.4)
                                  #(->dur-vec "y" TimeUnit/MICROSECOND 5))))

        (t/is (= {:res [(Duration/parse "PT3S")]
                  :vec-type DurationVector}
                 (test-projection '/
                                  #(->dur-vec "x" TimeUnit/SECOND 10)
                                  #(->bigint-vec "y" 3))))))))
