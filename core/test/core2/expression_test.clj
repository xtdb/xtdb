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
            [core2.util :as util])
  (:import [org.apache.arrow.vector BigIntVector Float4Vector Float8Vector IntVector SmallIntVector]
           org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(def a-field (ty/->field "a" ty/float8-type false))
(def b-field (ty/->field "b" ty/float8-type false))
(def d-field (ty/->field "d" ty/bigint-type false))
(def e-field (ty/->field "e" ty/varchar-type false))

(def data
  (for [n (range 1000)]
    {:a (double n), :b (double n), :d n, :e (format "%04d" n)}))

(t/deftest test-simple-projection
  (with-open [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
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

      (t/is (= (repeat 1000 0.0)
               (project '(if false a 0)))
            "if")

      (t/is (thrown? IllegalArgumentException (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
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
  (let [μs-2018 (util/instant->micros #c2/instant "2018")
        μs-2019 (util/instant->micros #c2/instant "2019")]
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
                             '{?tt #c2/instant "2019", ?vt #c2/instant "2018"}))))))))

(t/deftest test-date-trunc
  (with-open [node (node/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id "foo", :date #c2/instant "2021-01-21T12:34:56Z"}]])
          db (snap/snapshot (tu/component node ::snap/snapshot-factory) tx)]
      (t/is (= [{:trunc #c2/instant "2021-01-21"}]
               (op/query-ra '[:project [{trunc (date-trunc "DAY" date)}]
                              [:scan [date]]]
                            db)))

      (t/is (= [{:trunc #c2/instant "2021-01-21T12:34"}]
               (op/query-ra '[:project [{trunc (date-trunc "MINUTE" date)}]
                              [:scan [date]]]
                            db)))

      (t/is (= [{:trunc #c2/instant "2021-01-21"}]
               (op/query-ra '[:select (> trunc #inst "2021")
                              [:project [{trunc (date-trunc "DAY" date)}]
                               [:scan [date]]]]
                            db)))

      (t/is (= [{:trunc #c2/instant "2021-01-21"}]
               (op/query-ra '[:project [{trunc (date-trunc "DAY" trunc)}]
                              [:project [{trunc (date-trunc "MINUTE" date)}]
                               [:scan [date]]]]
                            db))))))

(defn- run-single-row-projection* [fields row form]
  (with-open [rel (tu/->relation (Schema. fields) [row])]
    (with-open [out-ivec (.project (expr/->expression-projection-spec "out" form {})
                                   tu/*allocator*
                                   rel)]
      (let [out-vec (.getVector out-ivec)]
        {:res (ty/get-object out-vec 0)
         :vec-type (class out-vec)}))))

(defn- run-single-row-projection
  ([f x]
   (run-single-row-projection* [(ty/->field "x" (ty/value->arrow-type x) false)]
                               {:x x}
                               (list f 'x)))

  ([f x y]
   (run-single-row-projection* [(ty/->field "x" (ty/value->arrow-type x) false)
                                (ty/->field "y" (ty/value->arrow-type y) false)]
                               {:x x, :y y}
                               (list f 'x 'y))))

(t/deftest test-mixing-numeric-types
  (t/is (= {:res 6, :vec-type IntVector}
           (run-single-row-projection '+ (int 4) (int 2))))

  (t/is (= {:res 6, :vec-type BigIntVector}
           (run-single-row-projection '+ (int 2) (long 4))))

  (t/is (= {:res 6, :vec-type SmallIntVector}
           (run-single-row-projection '+ (short 2) (short 4))))

  (t/is (= {:res 6.5, :vec-type Float4Vector}
           (run-single-row-projection '+ (byte 2) (float 4.5))))

  (t/is (= {:res 6.5, :vec-type Float4Vector}
           (run-single-row-projection '+ (float 2) (float 4.5))))

  (t/is (= {:res 6.5, :vec-type Float8Vector}
           (run-single-row-projection '+ (float 2) (double 4.5))))

  (t/is (= {:res 6.5, :vec-type Float8Vector}
           (run-single-row-projection '+ (int 2) (double 4.5))))

  (t/is (= {:res -2, :vec-type IntVector}
           (run-single-row-projection '- (short 2) (int 4))))

  (t/is (= {:res 8, :vec-type SmallIntVector}
           (run-single-row-projection '* (byte 2) (short 4))))

  (t/is (= {:res 2, :vec-type SmallIntVector}
           (run-single-row-projection '/ (short 4) (byte 2))))

  (t/is (= {:res 2.0, :vec-type Float4Vector}
           (run-single-row-projection '/ (float 4) (int 2)))))

(t/deftest test-throws-on-overflow
  (t/is (thrown? ArithmeticException
                 (run-single-row-projection '+ (Integer/MAX_VALUE) (int 4))))

  (t/is (thrown? ArithmeticException
                 (run-single-row-projection '- (Integer/MIN_VALUE) (int 4))))

  (t/is (thrown? ArithmeticException
                 (run-single-row-projection '- (Integer/MIN_VALUE))))

  (t/is (thrown? ArithmeticException
                 (run-single-row-projection '* (Integer/MIN_VALUE) (int 2))))

  #_ ; TODO this one throws IAE because that's what clojure.lang.Numbers/shortCast throws
  ;; the others are thrown by java.lang.Math/*Exact, which throw ArithmeticException
  (t/is (thrown? ArithmeticException
                 (run-single-row-projection '- (Short/MIN_VALUE)))))
