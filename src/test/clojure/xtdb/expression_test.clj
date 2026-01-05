(ns xtdb.expression-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.generators :as tcg]
            [clojure.test.check.properties :as tcp]
            [clojure.tools.logging :as log]
            [xtdb.expression :as expr]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.nio ByteBuffer)
           (java.time Duration Instant InstantSource LocalDate LocalDateTime LocalTime Period ZoneId ZonedDateTime)
           (java.time.temporal ChronoUnit)
           org.apache.arrow.vector.types.TimeUnit
           (xtdb.arrow RelationReader Vector VectorReader)
           (xtdb.time Interval)
           (xtdb.util StringUtil)))

(t/use-fixtures :each tu/with-allocator)

(defmethod expr/codegen-call [:dbg :any] [{:keys [tag], [arg-type] :arg-types}]
  {:return-col-type arg-type
   :->call-code (fn [[arg]]
                  `(doto ~arg
                     (->> pr-str (log/debugf "%s: %s" ~(if tag (format " (%s)" tag) "")))))})

(defn ->data-vecs []
  {:a (map double (range 1000))
   :b (map double (range 1000))
   :d (range 1000)
   :e (map #(format "%04d" %) (range 1000))})

(t/deftest test-simple-projection
  (with-open [in-rel (tu/open-rel (->data-vecs))]
    (letfn [(project [form]
              (let [input-types {:vec-fields {'a #xt/field {"a" :f64}, 'b #xt/field {"b" :f64}, 'd #xt/field {"d" :i64}}, :param-fields {}}
                    expr (expr/form->expr form input-types)]
                (with-open [project-col (.project (expr/->expression-projection-spec "c" expr input-types)
                                                  tu/*allocator* in-rel
                                                  {}
                                                  vw/empty-args)]
                  (.getAsList project-col))))]

      (t/is (= (mapv (comp double +) (range 1000) (range 1000))
               (project '(+ a b))))

      (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
               (project '(- a (* 2.0 b)))))

      (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
               (project '(:+ a (:+ b 2))))
            "support keywords")

      (t/is (= (mapv + (repeat 2) (range 1000))
               (project '(+ 2 d)))
            "mixing types")

      (t/is (= (repeat 1000 true)
               (project '(== a d)))
            "predicate")

      (t/is (= (mapv #(Math/sin ^double %) (range 1000))
               (project '(sin a)))
            "math")

      (t/is (= (interleave (map float (range)) (repeat 500 0))
               (project '(if (== 0 (mod a 2)) (/ a 2) 0)))
            "if")

      (t/is (anomalous? [:incorrect nil] (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (tu/open-rel (->data-vecs))]
    (letfn [(select-relation [form vec-fields args-map]
              (with-open [arg-rel (tu/open-args args-map)]
                (let [input-types {:vec-fields vec-fields,
                                   :param-fields (->> arg-rel
                                                      (into {} (map (fn [^VectorReader col]
                                                                      [(symbol (.getName col))
                                                                       (.getField col)]))))}]
                  (alength (.select (expr/->expression-selection-spec (expr/form->expr form input-types) input-types)
                                    tu/*allocator* in-rel {} arg-rel)))))]

      (t/testing "selector"
        (t/is (= 500 (select-relation '(>= a 500) '{a #xt/field {"a" :f64}} {})))
        (t/is (= 500 (select-relation '(>= e "0500") '{e #xt/field {"e" :utf8}} {}))))

      (t/testing "parameter"
        (t/is (= 500 (select-relation '(>= a ?a) '{a #xt/field {"a" :f64}} {:a 500})))
        (t/is (= 500 (select-relation '(>= e ?e) '{e #xt/field {"e" :utf8}} {:e "0500"})))))))

(t/deftest nil-selection-doesnt-yield-the-row
  (t/is (= 0
           (-> (.select (expr/->expression-selection-spec (expr/form->expr '(and true nil) {}) {})
                        tu/*allocator* (vr/rel-reader [] 1) {} vw/empty-args)
               (alength)))))

(defn project
  "Use to test an expression on some example documents. See also, project1.

  Usage: (project '(+ a b) [{:a 1, :b 2}, {:a 3, :b 4}]) ;; => [3, 7]"
  [expr docs]
  (let [docs (map-indexed #(assoc %2 :_id %1) docs)
        lp [:project [{'ret expr}] [:table docs]]]
    (mapv :ret (tu/query-ra lp {:default-tz #xt/zone "Z"}))))

(defn project1 [expr doc] (first (project expr [doc])))

(t/deftest test-variadic-and-or-94
  (t/is (= [true] (project '(and) [{}])))
  (t/is (= [false] (project '(or) [{}])))

  (t/is (= [true false] (project '(and x) [{:x true} {:x false}])))
  (t/is (= [true false] (project '(or x) [{:x true} {:x false}])))

  (t/is (= [true false false]
           (project '(and x y z)
                    [{:x true, :y true, :z true}
                     {:x false, :y true, :z true}
                     {:x true, :y true, :z false}])))

  (t/is (= [false true true]
           (project '(or x y z)
                    [{:x false, :y false, :z false}
                     {:x true, :y false, :z false}
                     {:x false, :y false, :z true}]))))

(t/deftest test-date-trunc-zoned-date-time
  (let [test-doc {:_id :foo,
                  :date (time/->instant #inst "2021-10-21T12:34:56.111111Z")
                  :zdt (-> (time/->zdt #inst "2021-08-21T12:34:56Z")
                           (.withZoneSameLocal (ZoneId/of "Europe/London")))}]
    (letfn [(simple-trunc [time-unit] (project1 (list 'date-trunc time-unit 'date) test-doc))]
      (t/is (= (time/->zdt #inst "2021-10-21T12:34:56.111111") (simple-trunc "MICROSECOND")))
      (t/is (= (time/->zdt #inst "2021-10-21T12:34:56.111") (simple-trunc "MILLISECOND")))
      (t/is (= (time/->zdt #inst "2021-10-21T12:34:56") (simple-trunc "SECOND")))
      (t/is (= (time/->zdt #inst "2021-10-21T12:34") (simple-trunc "MINUTE")))
      (t/is (= (time/->zdt #inst "2021-10-21T12:00") (simple-trunc "HOUR")))
      (t/is (= (time/->zdt #inst "2021-10-21") (simple-trunc "DAY")))
      (t/is (= (time/->zdt #inst "2021-10-18") (simple-trunc "WEEK")))
      (t/is (= (time/->zdt #inst "2021-10-01") (simple-trunc "MONTH")))
      (t/is (= (time/->zdt #inst "2021-10-01") (simple-trunc "QUARTER")))
      (t/is (= (time/->zdt #inst "2021-01-01") (simple-trunc "YEAR")))
      (t/is (= (time/->zdt #inst "2020-01-01") (simple-trunc "DECADE")))
      (t/is (= (time/->zdt #inst "2000-01-01") (simple-trunc "CENTURY")))
      (t/is (= (time/->zdt #inst "2000-01-01") (simple-trunc "MILLENNIUM"))))

    (t/is (= (-> (time/->zdt #inst "2021-08-21")
                 (.withZoneSameLocal (ZoneId/of "Europe/London")))
             (project1 '(date-trunc "DAY" zdt) test-doc))
          "timezone aware")

    (t/is (= (time/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" date) test-doc)))

    (t/is (= (time/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" (date-trunc "MINUTE" date)) test-doc)))))

(t/deftest test-date-trunc-local-date-time
  (t/testing "java.time.LocalDateTime"
    (let [ld (LocalDateTime/of 2022 4 3 12 34 56 789456999)
          trunc #(project1 (list 'date-trunc % 'date) {:date ld})]
      (t/is (= (LocalDateTime/of 2022 4 3 12 34 56 789456000) (trunc "MICROSECOND")))
      (t/is (= (LocalDateTime/of 2022 4 3 12 34 56 789000000) (trunc "MILLISECOND")))
      (t/is (= (LocalDateTime/of 2022 4 3 12 34 56) (trunc "SECOND")))
      (t/is (= (LocalDateTime/of 2022 4 3 12 34) (trunc "MINUTE")))
      (t/is (= (LocalDateTime/of 2022 4 3 12 0) (trunc "HOUR")))
      (t/is (= (LocalDateTime/of 2022 4 3 0 0) (trunc "DAY")))
      (t/is (= (LocalDateTime/of 2022 4 1 0 0) (trunc "MONTH")))
      (t/is (= (LocalDateTime/of 2022 1 1 0 0) (trunc "YEAR")))
      (t/is (= (LocalDateTime/of 2000 1 1 0 0) (trunc "MILLENNIUM")))
      (t/is (= (LocalDateTime/of 2000 1 1 0 0) (trunc "CENTURY")))
      (t/is (= (LocalDateTime/of 2020 1 1 0 0) (trunc "DECADE")))
      (t/is (= (LocalDateTime/of 2022 4 1 0 0) (trunc "QUARTER")))
      (t/is (= (LocalDateTime/of 2022 3 28 0 0) (trunc "WEEK"))))))

(t/deftest test-date-trunc-local-date
  (t/testing "java.time.LocalDate"
    (let [ld (LocalDate/of 2022 3 29)
          trunc #(project1 (list 'date-trunc % 'date) {:date ld})]
      (t/is (= (LocalDate/of 2022 3 29) (trunc "MICROSECOND")))
      (t/is (= (LocalDate/of 2022 3 29) (trunc "MILLISECOND")))
      (t/is (= (LocalDate/of 2022 3 29) (trunc "SECOND")))
      (t/is (= (LocalDate/of 2022 3 29) (trunc "MINUTE")))
      (t/is (= (LocalDate/of 2022 3 29) (trunc "HOUR")))
      (t/is (= (LocalDate/of 2022 3 29) (trunc "DAY")))
      (t/is (= (LocalDate/of 2022 3 1) (trunc "MONTH")))
      (t/is (= (LocalDate/of 2022 1 1) (trunc "YEAR")))
      (t/is (= (LocalDate/of 2022 1 1) (project1 '(date-trunc "YEAR" (date-trunc "MONTH" date)) {:date ld})))
      (t/is (= (LocalDate/of 2000 1 1) (trunc "MILLENNIUM")))
      (t/is (= (LocalDate/of 2000 1 1) (trunc "CENTURY")))
      (t/is (= (LocalDate/of 2020 1 1) (trunc "DECADE")))
      (t/is (= (LocalDate/of 2022 1 1) (trunc "QUARTER")))
      (t/is (= (LocalDate/of 2022 3 28) (trunc "WEEK"))))))

(t/deftest test-date-trunc-with-timezone-opt
  (let [test-doc {:_id :foo,
                  :date (-> (time/->zdt #inst "2001-02-16T20:38:40Z")
                            (.withZoneSameInstant (ZoneId/of "America/New_York")))}]

    (t/is (= #xt/zoned-date-time "2001-02-16T08:00-05:00[America/New_York]"
             (project1 (list 'date-trunc "DAY" 'date "Australia/Sydney") test-doc)))))

(t/deftest test-date-trunc-interval
  (let [test-doc {:_id :foo,
                  :year-interval (Interval. (Period/of 1111 4 8) (Duration/parse "PT1H1M1.111111S"))
                  :interval (Interval. (Period/of 0 4 8) (Duration/parse "PT1H1M1.111111S"))}]

    (letfn [(trunc [time-unit] (project1 (list 'date-trunc time-unit 'interval) test-doc))]
      (t/is (= #xt/interval "P4M8DT1H1M1.111111S" (trunc "MICROSECOND")))
      (t/is (= #xt/interval "P4M8DT1H1M1.111S" (trunc "MILLISECOND")))
      (t/is (= #xt/interval "P4M8DT1H1M1S" (trunc "SECOND")))
      (t/is (= #xt/interval "P4M8DT1H1M" (trunc "MINUTE")))
      (t/is (= #xt/interval "P4M8DT1H" (trunc "HOUR")))
      (t/is (= #xt/interval "P4M8D" (trunc "DAY")))
      (t/is (= #xt/interval "P4M7D" (trunc "WEEK")))
      (t/is (= #xt/interval "P4M" (trunc "MONTH")))
      (t/is (= #xt/interval "P3M" (trunc "QUARTER"))))
    
    (letfn [(trunc [time-unit] (project1 (list 'date-trunc time-unit 'year-interval) test-doc))] 
      (t/is (= #xt/interval "P13332M" (trunc "YEAR")))
      (t/is (= #xt/interval "P13320M" (trunc "DECADE")))
      (t/is (= #xt/interval "P13200M" (trunc "CENTURY")))
      (t/is (= #xt/interval "P12000M" (trunc "MILLENNIUM"))))))

(t/deftest test-date-extract
  (letfn [(extract [part date-like] (project1 (list 'extract part 'date) {:date date-like}))
          (extract-all [part date-likes] (project (list 'extract part 'date) (map (partial array-map :date) date-likes)))]
    (t/testing "java.time.Instant"
      (let [inst (time/->instant #inst "2022-03-21T13:44:52.344")] 
        (t/is (= 52 (extract "SECOND" inst)))
        (t/is (= 44 (extract "MINUTE" inst)))
        (t/is (= 13 (extract "HOUR" inst)))
        (t/is (= 21 (extract "DAY" inst)))
        (t/is (= 3 (extract "MONTH" inst)))
        (t/is (= 2022 (extract "YEAR" inst)))))

    (t/testing "java.time.ZonedDateTime"
      (let [zdt (-> (time/->zdt #inst "2022-03-21T13:44:52.344")
                    (.withZoneSameLocal (ZoneId/of "Asia/Calcutta")))] 
        (t/is (= 52 (extract "SECOND" zdt)))
        (t/is (= 44 (extract "MINUTE" zdt)))
        (t/is (= 13 (extract "HOUR" zdt)))
        (t/is (= 21 (extract "DAY" zdt)))
        (t/is (= 3 (extract "MONTH" zdt)))
        (t/is (= 2022 (extract "YEAR" zdt)))))
    
    (t/testing "java.time.LocalDateTime"
      (let [ldt (LocalDateTime/of 2022 4 3 12 34 56 789456999)]
        (t/is (= 56 (extract "SECOND" ldt)))
        (t/is (= 34 (extract "MINUTE" ldt)))
        (t/is (= 12 (extract "HOUR" ldt)))
        (t/is (= 3 (extract "DAY" ldt)))
        (t/is (= 4 (extract "MONTH" ldt)))
        (t/is (= 2022 (extract "YEAR" ldt)))))

    (t/testing "java.time.LocalDate"
      (let [ld (LocalDate/of 2022 03 21)]
        (t/is (anomalous? [:incorrect nil
                           #"Extract \"SECOND\" not supported for type date"]
                          (extract "SECOND" ld)))
        (t/is (= 21 (extract "DAY" ld)))
        (t/is (= 3 (extract "MONTH" ld)))
        (t/is (= 2022 (extract "YEAR" ld)))))

    (t/testing "mixed types"
      (let [dates [(time/->instant #inst "2022-03-22T13:44:52.344")
                   (-> (time/->zdt #inst "2021-02-23T21:19:10.692")
                       (.withZoneSameLocal (ZoneId/of "Asia/Calcutta")))
                   (LocalDate/of 2020 04 18)]] 
        (t/is (= [22 23 18] (extract-all "DAY" dates)))
        (t/is (= [3 2 4] (extract-all "MONTH" dates)))
        (t/is (= [2022 2021 2020] (extract-all "YEAR" dates))))))) 

(t/deftest test-interval-extract
  (letfn [(extract [part interval-val] (project1 (list 'extract part 'interval) {:interval interval-val}))]
    (let [itvl (Interval. (Period/of 1 4 8) (Duration/parse "PT3H10M12.1S"))]
      (t/is (= 12 (extract "SECOND" itvl)))
      (t/is (= 10 (extract "MINUTE" itvl)))
      (t/is (= 3 (extract "HOUR" itvl)))
      (t/is (= 8 (extract "DAY" itvl)))
      (t/is (= 4 (extract "MONTH" itvl)))
      (t/is (= 1 (extract "YEAR" itvl))))))

(t/deftest test-time-extract
  (letfn [(extract [part time-val] (project1 (list 'extract part 'time) {:time time-val}))]
    (let [tm (LocalTime/of 3 23 20)]
      (t/is (= 20 (extract "SECOND" tm)))
      (t/is (= 23 (extract "MINUTE" tm)))
      (t/is (= 3 (extract "HOUR" tm)))

      (t/is (anomalous? [:incorrect nil
                         #"Extract \"DAY\" not supported for type time without timezone"]
                        (extract "DAY" tm)))
      
      (t/is (anomalous? [:incorrect nil
                         #"Extract \"TIMEZONE_HOUR\" not supported for type time without timezone"]
                        (extract "TIMEZONE_HOUR" tm))))))

(t/deftest test-timezone-extract
  (letfn [(extract [part value] (project1 (list 'extract part 'value) {:value value}))]
    (t/testing "java.time.ZonedDateTime"
      (let [zdt (-> (time/->zdt #inst "2022-03-21T13:44:52.344")
                    (.withZoneSameLocal (ZoneId/of "Asia/Calcutta")))]
        (t/is (= 5 (extract "TIMEZONE_HOUR" zdt)))
        (t/is (= 30 (extract "TIMEZONE_MINUTE" zdt)))))
    
    (t/testing "java.time.Instant"
      (let [inst (time/->instant #inst "2022-03-21T13:44:52.344")]
        (t/is (= 0 (extract "TIMEZONE_HOUR" inst)))
        (t/is (= 0 (extract "TIMEZONE_MINUTE" inst))))) 

    (t/testing "type that doesn't support timezone fields"
      (let [ld (LocalDate/of 2022 03 21)]
        (t/is (anomalous? [:incorrect nil
                           #"Extract \"TIMEZONE_HOUR\" not supported for type date"]
                          (extract "TIMEZONE_HOUR" ld)))
        (t/is (anomalous? [:incorrect nil
                           #"Extract \"TIMEZONE_MINUTE\" not supported for type date"]
                          (extract "TIMEZONE_MINUTE" ld)))))))

(defn run-projection [^RelationReader rel form]
  (let [vec-fields (->> rel
                        (into {} (map (juxt #(symbol (.getName ^VectorReader %))
                                            #(.getField ^VectorReader %)))))
        input-types {:vec-fields vec-fields, :param-fields {}}
        expr (expr/form->expr form input-types)]
    (with-open [out-ivec (.project (expr/->expression-projection-spec "out" expr input-types)
                                   tu/*allocator* rel {} vw/empty-args)]
      {:res (.toList out-ivec #xt/key-fn :kebab-case-keyword)
       :res-type (types/->type (.getField out-ivec))})))

(t/deftest test-nils
  (with-open [rel (tu/open-rel {:x [1 1 nil nil]
                                :y [2 nil 2 nil]})]
    (t/is (= [3 nil nil nil]
             (-> (run-projection rel '(+ x y))
                 :res)))))

(t/deftest test-method-too-large-147
  (letfn [(run-test [form]
            (with-open [rel (tu/open-rel {:a [1 nil 3]
                                          :b [1.2 5.3 nil]
                                          :c [2 nil 8]
                                          :d [3.4 nil 5.3]
                                          :e [8 5 3]})]
              (-> (run-projection rel form)
                  :res)))]

    ;; SLT select2

    (t/is (= [63.0 nil nil]
             (run-test '(+ a (* b 2) (* c 3) (* d 4) (* e 5)))))

    (t/is (= [1 5.3 3]
             (run-test '(coalesce a b c d e))))

    (t/is (= [false nil true]
             (run-test '(and (<> (coalesce a b c d e) 0)
                             (> c d)
                             (or (<= c (- d 2)) (>= c (+ d 2)))))))))

(t/deftest test-variadics
  (letfn [(run-test [f x y z]
            (with-open [rel (tu/open-rel [{:x x, :y y, :z z}])]
              (-> (run-projection rel (list f 'x 'y 'z))
                  :res first)))]

    (t/is (= 6 (run-test '+ 1 2 3)))
    (t/is (= 1 (run-test '- 4 2 1)))
    (t/is (true? (run-test '< 1 2 4)))
    (t/is (false? (run-test '> 4 1 2)))))

(t/deftest test-numeric-errors-503
  (t/is (thrown-with-msg? RuntimeException #"division by zero"
                          (project1 '(/ a 0) {:a 5})))
  (t/is (thrown-with-msg? RuntimeException #"division by zero"
                          (project1 '(/ a 0.0) {:a 5})))
  (t/is (thrown-with-msg? RuntimeException #"division by zero"
                          (project1 '(/ a 0) {:a 5.0})))
  (t/is (thrown-with-msg? RuntimeException #"overflow"
                          (project1 '(+ a 9223372036854775807) {:a 9223372036854775807})))
  (t/is (thrown-with-msg? RuntimeException #"overflow"
                          (project1 '(- a -9223372036854775807) {:a 9223372036854775807}))))

(defn- project-mono-value [f-sym val vec-type]
  (with-open [rel (vr/rel-reader [(tu/open-vec (types/->field vec-type "s") [val])])]
    (-> (run-projection rel (list f-sym 's))
        :res
        first)))

(t/deftest test-character-length
  (letfn [(len [s] (project1 (list 'character-length 'a) {:a s}))]
    (t/are [s]
        (= (.count (.codePoints s)) (len s))

      ""
      "a"
      "hello"
      "😀")

    (t/is (= nil (len nil)))))

(tct/defspec character-length-is-equiv-to-code-point-count-prop
  (tcp/for-all [^String s tcg/string]
    (= (.count (.codePoints s)) (project1 '(character-length a) {:a s}))))

(tct/defspec octet-length-is-equiv-to-byte-count-prop
  (tcp/for-all [^String s tcg/string]
    (= (alength (.getBytes s "utf-8")) (project1 '(octet-length a) {:a s}))))

(t/deftest test-octet-length
  (letfn [(len [s vec-type]
            (project-mono-value 'octet-length s vec-type))]
    (t/are [s] (= (alength (.getBytes s "utf-8")) (len s #xt/type :utf8) (len (.getBytes s) #xt/type :varbinary))
      ""
      "a"
      "hello"
      "😀")
    (t/is (= nil (len nil #xt/type :null)))))

(t/deftest test-length
  (letfn [(length [test-val] (project1 (list 'length 'a) {:a test-val}))]
    (t/testing "calling length on strings"
      (t/is (= 5 (length "hello")))
      (t/is (= 0 (length "")))
      (t/is (= 1 (length "a")))
      (t/is (= 1 (length "😀"))))
    
    (t/testing "calling length on varbinary"
      (t/is (= 0 (length (byte-array []))))
      (t/is (= 2 (length (byte-array [-33 -44]))))
      (t/is (= 1 (length (byte-array [-33])))))
    
    (t/testing "calling length on lists"
      (t/is (= 0 (length [])))
      (t/is (= 1 (length [""])))
      (t/is (= 4 (length ["a" 1 3 :5]))))
    
    (t/testing "calling length on sets"
      (t/is (= (length #{}) 0))
      (t/is (= (length #{:a}) 1))
      (t/is (= (length #{"1" "2"}) 2)))
    
    (t/testing "calling length on structs"
      (t/is (= 0 (length {})))
      (t/is (= 1 (length {:a 1})))
      (t/is (= 3 (length {:a 1 :b 2 :c 3})))
      (t/is (= 2 (length {:a 1 :b 2 :c nil}))))))

(t/deftest test-struct-length-handles-absents
  (with-open [rel (tu/open-rel [{:x {:xa (byte 42)}}
                                {:x {:xa (byte 42), :xb (byte 41)}}
                                {:x {:xb (byte 41)}}
                                {:x {:xa nil :xb nil}}])]
    (t/is (= {:res [1 2 1 0], :res-type #xt/type :i32}
             (run-projection rel '(length x))))))

(t/deftest test-like
  (t/are [s ptn expected-result]
      (= expected-result (project1 '(like a b) {:a s, :b ptn}))

    "" "" true
    "a" "" false
    "a" "a" true
    "a" "b" false
    "a" "_" true
    "a" "_a" false
    "a" "%" true
    "a" "_%" true
    "a" "__%" false

    "." "_" true
    ".." ".." true
    ".*" ".." false

    "foobar" "fo%" true
    "foobar" "%ar" true
    "foobar" "%f__b%" true

    "foobar" "foo" false
    "foobar" "__foobar" false
    "foobar" "%foobar" true
    "foobar" "%foobar%" true

    "%%" "%_" true
    "%%" "___" false
    "%___%" "%" true
    "%__" "%_%" true

    "" nil nil
    "a" nil nil
    nil nil nil
    nil "%" nil

    "A" "a" false
    "a" "A" false
    "A" "%" true)

  (t/testing "literal projection"
    (t/is (project1 (list 'like 's "%.+%ar") {:s "foo .+ bar"}))))

(t/deftest test-like-regex
  (t/are [s ptn expected-result]
      (= expected-result (project1 '(like-regex a b "") {:a s, :b ptn}))

    "" "" true
    "a" "" true

    "a" "a" true
    "a" "b" false

    "a" "_" false
    "a" "_a" false
    "a" "%" false
    "a" "_%" false
    "a" "__%" false

    "a" "." true
    "a" ".+" true
    "a" ".*" true

    nil nil nil
    "a" nil nil
    nil nil nil
    nil "%" nil

    "foo" "foo" true
    "foo" "^foo$" true
    "foo" "f" true
    "foo" "oo" true
    "foo" "ooo" false
    "foo" "^fo$" false
    "foo" ".*(f|o).*" true
    "foo" "(f|d).*" true
    "foo" "(o|d).*" true
    "foo" "^(o|d).*" false

    "." "." true
    "." "\\." true
    ".+" "^.+$" true
    ".+" "^\\.\\+$" true
    ".+" "^.\\+$" true
    "+" "\\+" true
    "aaaa" "^.+$" true

    "A" "a" false)

  (t/testing "flags"
    (t/are [s ptn flags expected-result]
        (= expected-result (project1 (list 'like-regex 'a 'b flags) {:a s, :b ptn}))

      "" "" "i" true
      "A" "a" "i" true
      "ABCD" "a" "i" true
      "a\nB\nc" "^B$" "" false
      "a\nB\nc" "^B$" "m" true
      "a\nB\nc" "^b$" "m" false
      "a\nB\nc" "^b$" "mi" true
      "a\nB\nc" "^b$" "im" true
      "a\nB\nc" "^b$" "i  zz\nm" true

      "a\nB\nc" "a.B.c" "" false
      "a\nB\nc" "a.B.c" "s" true
      "a\nB\nc" "a.b.c" "s" false
      "a\nB\nc" "a.b.c" "is" true))

  (t/testing "literal projection"
    (t/is (project1 (list 'like-regex 's ".+ar" "") {:s "foo bar"}))))

(t/deftest test-binary-like
  (let [p 37
        u 95]

    (t/are [s ptn expected-result]
        (= expected-result (project1 '(like a b) {:a (some-> s byte-array), :b (some-> ptn byte-array)}))

      [] [] true

      [0] [] false
      [0] [0] true
      [0] [1] false
      [0] [u] true
      [0] [u 0] false
      [0] [p] true
      [0] [u p] true
      [0] [u u p] false

      ;; 46 = . for re collision tests
      ;; * = 42
      [46] [u] true
      [46 46] [46 46] true
      [46 42] [46 46] false

      [64 33 -33 -100] [64 33 -33 p] true

      [] nil nil
      [0] nil nil
      nil [p] nil)

    (t/testing "literal projection"
      (t/is (project1 (list 'like 's (byte-array [p -33 -44])) {:s (byte-array [-22 -21 -21 -33 -44])})))))

(t/deftest test-like-on-newline-str-regress
  (t/is (not (project1 '(like a b) {:a "\n", :b ""}))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec binary-like-is-equiv-to-string-like-on-utf8-prop
  (tcp/for-all [^String s tcg/string
                ^String ptn (tcg/fmap str/join (tcg/vector (tcg/elements [tcg/string (tcg/return "_") (tcg/return "%")])))]
    (= (project1 '(like a b) {:a s, :b ptn})
       (project1 '(like a b) {:a (.getBytes s "utf-8"), :b (.getBytes ptn "utf-8")}))))

(t/deftest test-trim
  (t/testing "leading trims of $"
    (t/are [s expected] (= expected (project1 '(trim-leading a b) {:a s, :b "$"}))

      "" ""
      " " " "
      "a" "a"
      "a$" "a$"
      "$a" "a"
      "$$a" "a"
      "$$$" ""
      "$a$" "a$"
      "a$a" "a$a"
      "$a$a$" "a$a$"

      nil nil))

  (t/testing "trailing trims of $"
    (t/are [s expected] (= expected (project1 '(trim-trailing a b) {:a s, :b "$"}))

      "" ""
      " " " "
      "a" "a"
      "$a" "$a"
      "a$" "a"
      "a$$" "a"
      "$$$" ""
      "$a$" "$a"
      "a$a" "a$a"
      "$a$a$" "$a$a"

      nil nil))

  (t/testing "both trims of $"
    (t/are [s expected]
        (= expected (project1 '(trim a b) {:a s, :b "$"}))

      "" ""
      " " " "
      "a" "a"
      "$a" "a"
      "a$" "a"
      "$$a" "a"
      "a$$" "a"
      "$$$" ""
      "$a$" "a"
      "a$a" "a$a"
      "$a$a$" "a$a"

      nil nil))

  (t/testing "null trim char returns null"
    (t/are [trim-fn s expected]
        (= expected (project1 (list trim-fn 'a 'b) {:a s, :b nil}))

      'trim "a" nil
      'trim nil nil

      'trim-leading "a" nil
      'trim-leading nil nil

      'trim-trailing "a" nil
      'trim-trailing nil nil))

  (t/testing "extended char plane trim"
    (t/are [s trim-char expected]
        (= expected (project1 '(trim a b) {:a s, :b trim-char}))
      "" "😎" ""
      "😎a" "😎" "a"
      "😎a" "😎" "a")))

(defn- all-whitespace-to-spaces
  "all whitespace to space, regex replace misses some stuff,
  there are java chars that are considered 'isWhitespace' not match by regex \\s."
  [s]
  (str/join (remove #(Character/isWhitespace ^Character %) s)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec sql-trim-is-equiv-to-java-trim-on-space-prop
  (tcp/for-all [s (tcg/fmap (comp all-whitespace-to-spaces str/join) (tcg/vector (tcg/elements [tcg/string (tcg/return " ")])))]
    (and
     (= (str/trim s) (project1 '(trim a b) {:a s, :b " "}))
     (= (str/triml s) (project1 '(trim-leading a b) {:a s, :b " "}))
     (= (str/trimr s) (project1 '(trim-trailing a b) {:a s, :b " "})))))

(defn- btrim [trim-fn bin trim-octet]
  (some-> (project1 (list trim-fn 'a 'b) {:a (some-> bin byte-array), :b (some-> trim-octet vector byte-array)})
          expr/resolve-bytes
          vec))

(t/deftest test-binary-trim
  (t/testing "leading trims of 0"
    (t/are [bin expected]
        (= expected (btrim 'trim-leading bin 0))

      [] []
      ;; \space
      [32] [32]
      [42] [42]
      [42 0] [42 0]
      [0 42] [42]
      [0 0 42] [42]
      [0 0 0] []
      [0 42 0] [42 0]
      [42 0 42] [42 0 42]
      [0 42 0 42 0] [42 0 42 0]

      nil nil))

  (t/testing "trailing trims of 0"
    (t/are [bin expected]
        (= expected (btrim 'trim-trailing bin 0))

      [] []
      [32] [32]
      [42] [42]
      [0 42] [0 42]
      [42 0] [42]
      [42 0 0] [42]
      [0 0 0] []
      [0 42 0] [0 42]
      [42 0 42] [42 0 42]
      [0 42 0 42 0] [0 42 0 42]

      nil nil))

  (t/testing "both trims of 0"
    (t/are [bin expected]
        (= expected (btrim 'trim bin 0))

      [] []
      [32] [32]
      [42] [42]
      [0 42] [42]
      [42 0] [42]
      [0 0 42] [42]
      [42 0 0] [42]
      [0 0 0] []
      [0 42 0] [42]
      [42 0 42] [42 0 42]
      [0 42 0 42 0] [42 0 42]

      nil nil))

  (t/testing "null trim octet returns null"
    (t/are [trim-fn bin expected]
        (= expected (btrim trim-fn bin nil))

      'trim [42] nil
      'trim nil nil

      'trim-leading [42] nil
      'trim-leading nil nil

      'trim-trailing [42] nil
      'trim-trailing nil nil))

  (t/testing "numeric octet is permitted"
    ;; no defined behaviour for bigint / bigdec
    (t/are [trim-fn bin octet expected]
        (= expected (some-> (project1 (list trim-fn 'a 'b) {:a (some-> bin byte-array), :b octet})
                            expr/resolve-bytes
                            vec))

      'trim nil (byte 0) nil
      'trim nil (int 0) nil
      'trim nil (short 0) nil
      'trim nil (long 0) nil
      'trim nil (float 0) nil
      'trim nil (double 0) nil

      'trim-leading nil (byte 0) nil
      'trim-leading nil (int 0) nil
      'trim-leading nil (short 0) nil
      'trim-leading nil (long 0) nil
      'trim-leading nil (float 0) nil
      'trim-leading nil (double 0) nil

      'trim-trailing nil (byte 0) nil
      'trim-trailing nil (int 0) nil
      'trim-trailing nil (short 0) nil
      'trim-trailing nil (long 0) nil
      'trim-trailing nil (float 0) nil
      'trim-trailing nil (double 0) nil

      'trim [0 42 0] (byte 0) [42]
      'trim [0 42 0] (int 0) [42]
      'trim [0 42 0] (short 0) [42]
      'trim [0 42 0] (long 0) [42]
      'trim [0 42 0] (float 0) [42]
      'trim [0 42 0] (double 0) [42]

      'trim-leading [0 42 0] (byte 0) [42 0]
      'trim-leading [0 42 0] (int 0) [42 0]
      'trim-leading [0 42 0] (short 0) [42 0]
      'trim-leading [0 42 0] (long 0) [42 0]
      'trim-leading [0 42 0] (float 0) [42 0]
      'trim-leading [0 42 0] (double 0) [42 0]

      'trim-trailing [0 42 0] (byte 0) [0 42]
      'trim-trailing [0 42 0] (int 0) [0 42]
      'trim-trailing [0 42 0] (short 0) [0 42]
      'trim-trailing [0 42 0] (long 0) [0 42]
      'trim-trailing [0 42 0] (float 0) [0 42]
      'trim-trailing [0 42 0] (double 0) [0 42])))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec bin-trim-is-equiv-to-str-trim-on-utf8-prop
  (tcp/for-all [^String s (tcg/fmap (comp all-whitespace-to-spaces str/join) (tcg/vector (tcg/elements [tcg/string (tcg/return " ")])))]
    (and
     (= (str/trim s)
        (String. (byte-array (btrim 'trim (.getBytes s "utf-8") 32)) "utf-8"))
     (= (str/triml s)
        (String. (byte-array (btrim 'trim-leading (.getBytes s "utf-8") 32)) "utf-8"))
     (= (str/trimr s)
        (String. (byte-array (btrim 'trim-trailing (.getBytes s "utf-8") 32)) "utf-8")))))

(t/deftest test-upper
  (t/are [s expected]
      (= expected (project1 '(upper a) {:a s}))
    nil nil
    "" ""
    " " " "
    "a" "A"
    "aa" "AA"
    "AA" "AA"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec upper-is-equiv-to-java-upper-prop
  (tcp/for-all [^String s tcg/string]
    (= (.toUpperCase s) (project1 '(upper a) {:a s}))))

(t/deftest test-lower
  (t/are [s expected]
      (= expected (project1 '(lower a) {:a s}))
    nil nil
    "" ""
    " " " "
    "A" "a"
    "AA" "aa"
    "aa" "aa"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec lower-is-equiv-to-java-lower-prop
  (tcp/for-all [^String s tcg/string]
    (= (.toLowerCase s) (project1 '(lower a) {:a s}))))

(t/deftest concat-test
  (t/are [s1 s2 expected]
      (= expected (project1 '(concat a b) {:a s1, :b s2}))

    nil nil nil
    "" nil nil
    nil "" nil
    "" "" ""
    "a" "b" "ab")

  (t/is (= ["a1__" "a2__"] (project '(concat a "__") [{:a "a1"}, {:a "a2"}]))
        "resets position of literal buffer"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec concat-equiv-to-str-prop
  (tcp/for-all [s1 tcg/string
                s2 tcg/string]
    (= (str s1 s2) (project1 '(concat a b) {:a s1 :b s2}))))

(defn- bconcat [b1 b2]
  (some-> (project1 '(concat a b) {:a (some-> b1 byte-array),
                                   :b (some-> b2 byte-array)})
          expr/resolve-bytes
          vec))

(t/deftest bin-concat-test
  (t/are [b1 b2 expected]
      (= expected (bconcat b1 b2))
    nil nil nil
    [] nil nil
    nil [] nil
    [] [] []
    [42] [32] [42 32]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec bin-concat-equiv-to-str-concat-on-utf8-prop
  (tcp/for-all [^String s1 tcg/string
                ^String s2 tcg/string]
    (= (str s1 s2) (String. (byte-array (bconcat (.getBytes s1 "utf-8") (.getBytes s2 "utf-8"))) "utf-8"))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec variadic-str-concat-prop
  (tcp/for-all [^String strs (tcg/vector tcg/string 2 99)]
    (= (apply str strs) (project1 (list* 'concat strs) {}))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec variadic-bin-concat-prop
  (tcp/for-all [^bytes barrs (tcg/vector tcg/bytes 2 99)]
    (= (vec (apply concat barrs)) (vec (expr/resolve-bytes (project1 (list* 'concat barrs) {}))))))

(t/deftest position-test
  (t/are [pos-fn s1 s2 expected]
      (= expected (project1 (list pos-fn 'a 'b) {:a s1, :b s2}))

    'position nil nil nil
    'position nil "" nil
    'position "" nil nil

    'position "" "" 1
    'octet-position "" "" 1

    'position "a" "" 0
    'position "" "a" 1
    'position "a" "a" 1
    'position "b" "a" 0

    'octet-position "a" "" 0
    'octet-position "" "a" 1
    'octet-position "a" "a" 1
    'octet-position "b" "a" 0

    'position "😎" "😎" 1
    'position "😎" "a😎" 2
    'position "😎" "🌝😎" 2

    'octet-position "😎" "😎" 1
    'octet-position "😎" "a😎" 2
    'octet-position "😎" "aa😎" 3
    'octet-position "😎" "🌝😎" 5))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec position-is-codepoint-count-from-idx-prop
  (tcp/for-all [s1 tcg/string
                s2 tcg/string]
    (let [pos (project1 '(position a b) {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc (Character/codePointCount (str s1) (int 0) (int i))))
        (zero? pos)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec position-is-equiv-to-idx-of-on-ascii-prop
  (tcp/for-all [s1 tcg/string-ascii
                s2 tcg/string-ascii]
    (let [pos (project1 '(position a b) {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc i))
        (zero? pos)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec position-on-octet-is-equiv-to-idx-of-on-ascii-prop
  (tcp/for-all [s1 tcg/string-ascii
                s2 tcg/string-ascii]
    (let [pos (project1 '(octet-position a b) {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc i))
        (zero? pos)))))

(t/deftest binary-position-test
  (t/are [b1 b2 expected]
      (= expected (project1 '(octet-position a b) {:a (some-> b1 byte-array), :b (some-> b2 byte-array)}))
    nil nil nil
    [] [] 1
    [42] [] 0
    [] [42] 1
    [42] [42] 1
    [43] [42] 0
    [42] [43] 0
    [-44 21] [-32 -44 -21] 0
    [-44 -21] [-32 -44 -21] 2))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec binary-position-equiv-to-octet-position-prop
  (tcp/for-all [^String s1 tcg/string
                ^String s2 tcg/string]
    (= (project1 '(octet-position a b) {:a s1, :b s2})
       (project1 '(octet-position a b) {:a (.getBytes s1 "utf-8"), :b (.getBytes s2 "utf-8")}))))

(t/deftest substring-test
  (t/are [s pos len expected]
      (= expected (project1 '(substring s pos len) {:s s, :pos pos, :len len}))

    "" -1 0 ""
    "" 0 0 ""
    "" 1 0 ""
    "" 1 1 ""
    "" 1 2 ""

    "a" -1 0 ""
    "a" -1 1 ""
    "a" -1 2 ""
    "a" -1 3 "a"

    "a" 1 0 ""
    "a" 1 1 "a"
    "a" 1 2 "a"

    "🌝😎😎" 2 2 "😎😎"
    "f😎😎bar" 1 1 "f"
    "f😎😎bar" 2 1 "😎"
    "f😎😎bar" 2 100 "😎😎bar"

    "1234567890" 3 8 "34567890"
    "1234567890" 4 3 "456"

    "string" 2 2147483646 "tring"
    "string" -10 2147483646 "string"))

(t/deftest negative-substring-length-test
  (t/is (anomalous? [:incorrect nil #"Negative substring length"] (project1 '(substring "" 0 -1) {}))))

(t/deftest substring-nils-test
  (doseq [a ["" nil]
          b [1 nil]
          c [1 nil]
          :when (not (and a b c))]
    (t/is (nil? (project1 '(substring a b c) {:a a, :b b, :c c})))))

(defn- utf8len [^String s] (StringUtil/utf8Length (ByteBuffer/wrap (.getBytes s "utf-8"))))

(defn- substring-args-gen [string-gen]
  (tcg/bind string-gen
            (fn [s] (tcg/tuple (tcg/return s)
                               (tcg/choose 1 (inc (utf8len s)))
                               (tcg/choose 0 (utf8len s))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec substring-with-no-len-is-equiv-to-remaining-str-prop
  (tcp/for-all [[s i] (substring-args-gen tcg/string)]
    (= (project1 '(substring a b c) {:a s, :b i, :c (- (utf8len s) (dec i))})
       (project1 '(substring a b) {:a s, :b i}))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec substring-is-equiv-to-clj-on-ascii-when-idx-within-bounds-prop
  (tcp/for-all [[s i len] (substring-args-gen tcg/string-ascii)]
    (= (subs s (dec i) (min (+ (dec i) len) (count s)))
       (project1 '(substring a b c) {:a s, :b i, :c len}))))

(t/deftest bin-substring-test
  (t/are [s pos len expected]
      (= expected (vec (expr/resolve-bytes (project1 '(substring a b c) {:a (some-> s byte-array), :b pos, :c len}))))

    [] -1 0 []
    [] 0 0 []
    [] 1 0 []
    [] 1 1 []
    [] 1 2 []

    [0] -1 0 []
    [0] -1 1 []
    [0] -1 2 []
    [0] -1 3 [0]

    [0] 1 0 []
    [0] 1 1 [0]
    [0] 1 2 [0]

    [1 2 3 4 5 6 7 8 9 0] 3 8 [3 4 5 6 7 8 9 0]
    [1 2 3 4 5 6 7 8 9 0] 4 3 [4 5 6]

    [1 2 3 4 5] 2 2147483646 [2 3 4 5]
    [1 2 3 4 5] -10 2147483646 [1 2 3 4 5]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec bin-substring-is-equiv-to-substring-on-ascii-prop
  (tcp/for-all [[s i len] (substring-args-gen tcg/string-ascii)]
    (= (vec (expr/resolve-bytes (project1 '(substring a b c) {:a (.getBytes ^String s "ascii"), :b i, :c len})))
       (vec (.getBytes ^String (project1 '(substring a b c) {:a s, :b i, :c len}) "ascii")))))

(t/deftest overlay-test
  (t/are [s1 s2 from len expected]
      (= expected (project1 '(overlay a b c d) {:a s1, :b s2, :c from, :d len}))

    "" "" 1 0 ""
    "" "" 1 1 ""
    "a" "b" 1 1 "b"
    "a" "b" 1 0 "ba"

    "foobar" "zzz" 1 0 "zzzfoobar"
    "foobar" "zzz" 1 1 "zzzoobar"
    "foobar" "zzz" 1 2 "zzzobar"
    "foobar" "zzz" 1 6 "zzz"
    "foobar" "zzz" 4 3 "foozzz"
    "foobar" "zzz" 4 4 "foozzz"

    "a" "bbb" 1 0 "bbba"
    "a" "bbb" 2 0 "abbb"

    "aaa" "" 1 1 "aa"

    "a" "bbb" 1 1 "bbb"

    "😎" "🌝😎" 1 1 "🌝😎"
    "🌝😎" "😎" 1 1 "😎😎"
    "🌝😎" "😎" 2 0 "🌝😎😎"))

(t/deftest overlay-negative-substring-length-test
  (t/is (anomalous? [:incorrect nil #"Negative substring length"] (project1 '(overlay "" "" 0 0) {}))))

(t/deftest overlay-nils-test
  (doseq [a ["" nil]
          b ["" nil]
          c [1 nil]
          d [1 nil]
          :when (not (and a b c d))]
    (t/is (nil? (project1 '(overlay a b c d) {:a a, :b b, :c c, :d d})))))

(defn- overlay-args-gen [string-gen]
  (-> string-gen
      (tcg/bind (fn [s]
                  (tcg/tuple
                   (tcg/return s)
                   (tcg/choose 0 (count s)))))
      (tcg/bind (fn [[s i]]
                  (tcg/tuple
                   (tcg/return s)
                   string-gen
                   (tcg/return (inc i))
                   (tcg/choose 0 (- (count s) i)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec overlay-is-equiv-to-ss-concat-on-ascii-prop
  (tcp/for-all [[s1 s2 i len] (overlay-args-gen tcg/string-ascii)]
    (= (str (subs s1 0 (dec i)) s2 (subs s1 (+ (dec i) len) (count s1)))
       (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d len}))))

(t/deftest binary-overlay-test
  (t/are [s1 s2 from len expected]
      (= expected (vec (expr/resolve-bytes (project1 '(overlay a b c d) {:a (some-> s1 byte-array), :b (some-> s2 byte-array), :c from, :d len}))))

    [] [] 1 0 []
    [] [] 1 1 []
    [0] [1] 1 1 [1]
    [0] [1] 1 0 [1 0]

    [0 1 2 3 4] [5 5 5] 1 0 [5 5 5 0 1 2 3 4]
    [0 1 2 3 4] [5 5 5] 1 1 [5 5 5 1 2 3 4]
    [0 1 2 3 4] [5 5 5] 1 2 [5 5 5 2 3 4]
    [0 1 2 3 4] [5 5 5] 1 5 [5 5 5]
    [0 1 2 3 4] [5 5 5] 3 3 [0 1 5 5 5]

    [0] [1 1 1] 1 0 [1 1 1 0]
    [0] [1 1 1] 2 0 [0 1 1 1]

    [0 0 0] [] 1 1 [0 0]
    [0] [1 1 1] 1 1 [1 1 1]))

(t/deftest binary-overlay-nils-tset
  (doseq [a [[] nil]
          b [[] nil]
          c [1 nil]
          d [1 nil]
          :when (not (and a b c d))]
    (t/is (nil? (project1 '(overlay a b c d) {:a (some-> a byte-array), :b (some-> b byte-array), :c c, :d d})))))

(tct/defspec binary-overlay-is-equiv-to-str-overlay-on-ascii-prop
  (tcp/for-all [[s1 s2 i len] (overlay-args-gen tcg/string-ascii)]
    (= (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d len})
       (String. (expr/resolve-bytes (project1 '(overlay a b c d) {:a (.getBytes ^String s1 "ascii"),
                                                                  :b (.getBytes ^String s2 "ascii"),
                                                                  :c i,
                                                                  :d len}))
                "ascii"))))

(tct/defspec overlay-len-default-is-len-of-placing-prop
  (tcp/for-all [[s1 s2 i] (overlay-args-gen tcg/string)]
    (= (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d (project1 '(character-length a) {:a s2})})
       (project1 '(overlay a b c (default-overlay-length b)) {:a s1, :b s2, :c i}))))

(tct/defspec binary-overlay-len-default-is-len-of-placing-prop
  (tcp/for-all [[s1 s2 i] (overlay-args-gen tcg/bytes)]
    (= (util/->clj (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d (project1 '(octet-length a) {:a s2})}))
       (util/->clj (project1 '(overlay a b c (default-overlay-length b)) {:a s1, :b s2, :c i})))))

(t/deftest test-math-functions
  (t/is (= [1.4142135623730951 1.8439088914585775 nil]
           (project '(sqrt x) [{:x 2} {:x 3.4} {:x nil}])))
  (t/is (= [0.9092974268256817 -0.2555411020268312 nil]
           (project '(sin x) [{:x 2} {:x 3.4} {:x nil}])))
  (t/is (= [2 3.4 5.0 nil]
           (project '(abs x) [{:x -2} {:x -3.4} {:x 5.0} {:x nil}])))
  (t/is (= [4.0 11.559999999999999 nil nil nil]
           (project '(power x y) [{:x -2 :y 2} {:x -3.4 :y 2.0} {:x 5.0 :y nil} {:x nil :y 2} {:x nil :y nil}])))
  (t/is (= [2.1760912590556813 nil]
           (project '(log10 x) [{:x 150} {:x nil}])))
  (t/is (= [4.0 nil nil nil]
           (project '(log y x) [{:x 16 :y 2} {:x nil :y 2} {:x 16 :y nil} {:x nil :y nil}])))
  (t/is (= [2.772588722239781 nil]
           (project '(ln x) [{:x 16} {:x nil}]))))

(t/deftest test-least-greatest
  (letfn [(run-test [form x y]
            (with-open [rel (tu/open-rel [{:x x, :y y}])]
              (-> (run-projection rel form)
                  :res first)))]
    (t/is (= 9 (run-test '(greatest x y) 1 9)))
    (t/is (= 1.0 (run-test '(least x y) 1.0 9.0)))

    (t/is (= nil (run-test '(least x y) nil 9.0)))

    (t/is (= nil (run-test '(greatest x y) 1.0 nil)))

    (t/testing "mixed temporal types"
      (t/is (= #xt/date "2020-08-02"
               (run-test '(least x y) #xt/date-time "2020-08-02T15:09:00" #xt/date "2020-08-02")))

      (t/is (= #xt/date-time "2020-08-01T15:09:00"
               (run-test '(least x y) #xt/date-time "2020-08-01T15:09:00" #xt/date "2020-08-02"))))))

(t/deftest test-random
  (t/is (= [true] (project '(<= 0.0 (random)) [{}])))
  (t/is (= [true] (project '(< (random) 1.0) [{}]))))

(t/deftest can-return-string-multiple-times
  (with-open [rel (tu/open-rel {:x [1 2 3]})]
    (t/is (= {:res ["foo" "foo" "foo"]
              :res-type #xt/type :utf8}
             (run-projection rel "foo")))))

(t/deftest test-cond
  (letfn [(run-test [expr xs]
            (with-open [rel (tu/open-rel {:x xs})]
              (run-projection rel expr)))]

    (t/is (= {:res ["big" "small" "tiny" "tiny"]
              :res-type #xt/type :utf8}
             (run-test '(cond (> x 100) "big", (> x 10) "small", "tiny")
                       [500 50 5 nil])))

    (t/is (= {:res ["big" "small" nil nil]
              :res-type #xt/type [:? :utf8]}
             (run-test '(cond (> x 100) "big", (> x 10) "small")
                       [500 50 5 nil])))))

(t/deftest test-let
  (t/is (= {:res [6 9 12 nil]
            :res-type #xt/type [:? :i64]}
           (with-open [rel (tu/open-rel {:x [1 2 3 nil]})]
             (run-projection rel '(let [y (* x 2)
                                        y (+ y 3)]
                                    (+ x y)))))))

(t/deftest test-case
  (t/is (= {:res ["x=1" "x=2" "none of the above" "none of the above"]
            :res-type #xt/type :utf8}
           (with-open [rel (tu/open-rel {:x [1 2 3 nil]})]
             (run-projection rel '(case (* x 2)
                                    2 "x=1"
                                    (+ x 2) "x=2"
                                    "none of the above"))))))

(t/deftest test-coalesce
  (letfn [(run-test [expr]
            (with-open [rel (tu/open-rel {:x ["x" nil nil]
                                          :y ["y" "y" nil]})]
              (run-projection rel expr)))]

    (t/is (= {:res ["x" "y" nil]
              :res-type #xt/type [:? :utf8]}
             (run-test '(coalesce x y))))

    (t/is (= {:res ["x" "lit" "lit"]
              :res-type #xt/type :utf8}
             (run-test '(coalesce x "lit" y))))

    (t/is (= {:res ["x" "y" "default"]
              :res-type #xt/type :utf8}
             (run-test '(coalesce x y "default"))))))

(t/deftest test-nullif
  (letfn [(run-test [expr]
            (with-open [rel (tu/open-rel {:x ["x" "y" nil "x"]
                                          :y ["y" "y" nil nil]})]
              (run-projection rel expr)))]

    (t/is (= {:res ["x" nil nil "x"]
              :res-type #xt/type [:? :utf8]}
             (run-test '(nullif x y))))))

(t/deftest test-mixing-numeric-types
  (letfn [(run-test [f x y]
            (with-open [rel (tu/open-rel [{:x x, :y y}])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]

    (t/is (= {:res 6, :res-type #xt/type :i32}
             (run-test '+ (int 4) (int 2))))

    (t/is (= {:res 6, :res-type #xt/type :i64}
             (run-test '+ (int 2) (long 4))))

    (t/is (= {:res 6, :res-type #xt/type :i16}
             (run-test '+ (short 2) (short 4))))

    (t/is (= {:res 6.5, :res-type #xt/type :f32}
             (run-test '+ (byte 2) (float 4.5))))

    (t/is (= {:res 6.5, :res-type #xt/type :f32}
             (run-test '+ (float 2) (float 4.5))))

    (t/is (= {:res 6.5, :res-type #xt/type :f64}
             (run-test '+ (float 2) (double 4.5))))

    (t/is (= {:res 6.5, :res-type #xt/type :f64}
             (run-test '+ (int 2) (double 4.5))))

    (t/is (= {:res -2, :res-type #xt/type :i32}
             (run-test '- (short 2) (int 4))))

    (t/is (= {:res 8, :res-type #xt/type :i16}
             (run-test '* (byte 2) (short 4))))

    (t/is (= {:res 2, :res-type #xt/type :i16}
             (run-test '/ (short 4) (byte 2))))

    (t/is (= {:res 2.0, :res-type #xt/type :f32}
             (run-test '/ (float 4) (int 2))))))

(t/deftest test-decimal-arithmetic-and-coersion
  (letfn [(run-test [f x y]
            (with-open [rel (tu/open-rel [{:x x, :y y}])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]
    ;; standard ops
    (t/is (= {:res 2.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '+ (bigdec 1.0) (bigdec 1.0))))

    (t/is (= {:res 2.01M, :res-type #xt/type [:decimal 32 2 128]}
             (run-test '+  (bigdec 1.01) (bigdec 1.0)))
          "differnt scales take the larger")

    (t/is (= {:res 0.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '- (bigdec 1.0) (bigdec 1.0))))
    (t/is (= {:res 0.5M, :res-type #xt/type [:decimal 32 6 128]}
             (run-test '/ (bigdec 1.0) (bigdec 2.0))))
    (t/is (= {:res 2.00M, :res-type #xt/type [:decimal 32 2 128]}
             (run-test '* (bigdec 1.0) (bigdec 2.0))))
    (t/is (= {:res false, :res-type #xt/type :bool}
             (run-test '> (bigdec 1.0) (bigdec 2.0))))

    (t/is (= {:res 61954235709850086879078532699846656405640394575840079131297.39935M,
              :res-type #xt/type [:decimal 64 5 256]}
             (run-test '+ (bigdec 1.0)
                       61954235709850086879078532699846656405640394575840079131296.39935M)))

    ;; LUB
    (t/is (= {:res 2.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '* (byte 1) (bigdec 2.0))))
    (t/is (= {:res 2.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '* (short 1) (bigdec 2.0))))
    (t/is (= {:res 2.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '* (int 1) (bigdec 2.0))))
    (t/is (= {:res 2.0M, :res-type #xt/type [:decimal 32 1 128]}
             (run-test '* (bigdec 2.0) (int 1))))
    (t/is (= {:res true, :res-type #xt/type :bool}
             (run-test '< (int 1) (bigdec 2.0))))
    (t/is (= {:res false, :res-type #xt/type :bool}
             (run-test '> (int 1) (bigdec 2.0))))
    (t/is (= {:res 3.0, :res-type #xt/type :f32}
             (run-test '+ (float 1.0) (bigdec 2.0))))
    (t/is (= {:res 3.0, :res-type #xt/type :f64}
             (run-test '+ (double 1.0) (bigdec 2.0))))
    (t/is (= {:res (double 9.0) :res-type #xt/type :f64}
             (run-test 'power (double 3.0) (bigdec 2.0))))

    (t/is (= {:res (double 3.0) :res-type #xt/type :f64}
             (run-test 'log (double 2.0) (bigdec 8.0))))

    ;; least + greatest

    (t/is (= {:res 2.0, :res-type #xt/type [:union :f64 [:decimal 32 1 128]]}
             (run-test 'least (bigdec 3.0) (double 2.0))))
    (t/is (= {:res 3.0, :res-type #xt/type [:union :f64 [:decimal 32 1 128]]}
             (run-test 'greatest (double 3.0) (bigdec 2.0))))

    (t/is (= {:res-type #xt/type [:union [:decimal 32 2 128] [:decimal 32 1 128]],
              :res 1.0M}
             (run-test 'least (bigdec 1.0M) (bigdec 2.02))))

    ;; some edge cases

    (t/is (= {:res 1.0000000000000000000000000000001E+32M,
              :res-type #xt/type [:decimal 32 -1 128]}
             (run-test '+ (bigdec 1E+1M) (bigdec 1E+32M))))

    (t/is (thrown? RuntimeException
                   (run-test '+ (bigdec 1E+1M) (bigdec 1E+33M))))

    (t/is (= {:res 1001E+125M, :res-type #xt/type [:decimal 32 -125 128]}
             (run-test '+  (bigdec 1E+128M)  (bigdec 1E+125M))))

    (t/is (= {:res 2E+128M, :res-type #xt/type [:decimal 32 -128 128]}
             (run-test '+ (bigdec 1E+128M) (bigdec 1E+128M)))
          "negative scale")

    (t/testing "boundary cases"
      (t/testing "overflow"
        ;; largest power of 2 with 32 digits
        (let [boundary (-> (BigDecimal/valueOf 2) (.pow 106))]
          (t/is (thrown? RuntimeException
                         (run-test '+ boundary boundary))))
        (let [boundary (-> (BigDecimal/valueOf 2) (.pow 212))]
          (t/is (thrown? RuntimeException
                         (run-test '+ boundary boundary)))))

      (t/testing "underflow"
        (let [boundary (/ 1.0M (-> (BigDecimal/valueOf 2) (.pow 91)))]
          (t/is (thrown? RuntimeException
                         (run-test '/ boundary 2.0M)))))

      (t/testing "out of precision"
        (t/is (thrown? RuntimeException
                       (run-test '+ (bigdec 1E+1M) (bigdec 1E+65M))))))))

(t/deftest test-multi-decimal-arithmetic
  (letfn [(run-test [f xs yz]
            (with-open [rel (tu/open-rel {:x xs, :y yz})]
              (run-projection rel (list f 'x 'y))))]

    (t/is (= {:res [2.001M 2.0101M],
              :res-type #xt/type [:union [:decimal 32 4 128] [:decimal 32 3 128]]}
             (run-test '+
                       [(bigdec 1.0) (bigdec 1.01)]
                       [(bigdec 1.001) (bigdec 1.0001)])))

    (t/is (= #xt/type [:decimal 64 5 256]
             (-> (run-test '+
                           [(bigdec 1.0) (bigdec 1.01)]
                           (repeat 2 61954235709850086879078532699846656405640394575840079131296.39935M))
                 :res-type)))

    (t/is (= {:res [0.0 0.010000000000000009], :res-type #xt/type :f64}
             (run-test '-
                       [(bigdec 1.0) (bigdec 1.01)]
                       (repeat 2 (double 1.0)))))

    (t/is (= {:res-type #xt/type [:union [:decimal 32 2 128] [:decimal 32 1 128]],
              :res [0.0M 0.01M]}
             (run-test '-
                       [(bigdec 1.0) (bigdec 1.01)]
                       (repeat 2 1))))))


(t/deftest test-throws-on-overflow
  (letfn [(run-unary-test [f x]
            (with-open [rel (tu/open-rel [{:x x}])]
              (-> (run-projection rel (list f 'x))
                  (update :res first))))

          (run-binary-test [f x y]
            (with-open [rel (tu/open-rel [{:x x, :y y}])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]

    (t/is (thrown? RuntimeException
                   (run-binary-test '+ Integer/MAX_VALUE (int 4))))

    (t/is (thrown? RuntimeException
                   (run-binary-test '- Integer/MIN_VALUE (int 4))))

    (t/is (thrown? RuntimeException
                   (run-unary-test '- Integer/MIN_VALUE)))

    (t/is (thrown? RuntimeException
                   (run-binary-test '* Integer/MIN_VALUE (int 2))))

    #_ ; TODO this one throws IAE because that's what clojure.lang.Numbers/shortCast throws
    ;; the others are thrown by java.lang.Math/*Exact, which throw ArithmeticException
    (t/is (thrown? ArithmeticException
                   (run-unary-test '- (Short/MIN_VALUE))))))

(t/deftest test-polymorphic-columns
  (t/is (= {:res [1.2 1 3.4]
            :res-type #xt/type [:union :f64 :i64]}
           (with-open [rel (tu/open-rel {:x [1.2 1 3.4]})]
             (run-projection rel 'x))))

  (t/is (= {:res [4.4 9.75]
            :res-type #xt/type [:union :f64 :f32]}
           (with-open [rel (tu/open-rel {:x [1 1.5]
                                         :y [3.4 (float 8.25)]})]
             (run-projection rel '(+ x y)))))

  (t/is (= {:res [(float 4.4) nil nil nil]
            :res-type #xt/type [:union :f64 :f32 [:? :null]]}
           (with-open [rel (tu/open-rel {:x [1 12 nil nil]
                                         :y [(float 3.4) nil 4.8 nil]})]
             (run-projection rel '(+ x y))))))

(t/deftest test-ternary-booleans
  (t/is (= [{:res [true false nil false false false nil false nil]
             :res-type #xt/type [:? :bool]}
            {:res [true true true true false nil true nil nil]
             :res-type #xt/type [:? :bool]}]
           (with-open [rel (tu/open-rel {:x [true true true false false false nil nil nil]
                                         :y [true false nil true false nil true false nil]})]
             [(run-projection rel '(and x y))
              (run-projection rel '(or x y))])))

  (t/is (= [{:res [false true nil]
             :res-type #xt/type [:? :bool]}
            {:res [true false false]
             :res-type #xt/type :bool}
            {:res [false true false]
             :res-type #xt/type :bool}
            {:res [false false true]
             :res-type #xt/type :bool}]
           (with-open [rel (tu/open-rel {:x [true false nil]})]
             [(run-projection rel '(not x))
              (run-projection rel '(true? x))
              (run-projection rel '(false? x))
              (run-projection rel '(nil? x))]))))

(t/deftest test-mixing-timestamp-types
  (letfn [(->ts-vec [col-name time-unit, ^long value]
            (doto (tu/open-vec col-name (types/->type [:timestamp-tz time-unit "UTC"]) [])
              (.writeLong value)))

          (->dur-vec [col-name ^TimeUnit time-unit, ^long value]
            (doto (tu/open-vec col-name (types/->type [:duration time-unit]) [])
              (.writeLong value)))

          (test-projection [f-sym ->x-vec ->y-vec]
            (with-open [^Vector x-vec (->x-vec)
                        ^Vector y-vec (->y-vec)]
              (run-projection (vr/rel-reader [x-vec y-vec])
                              (list f-sym 'x 'y))))]

    (t/testing "ts/dur"
      (t/is (= {:res [(time/->zdt #inst "2021-01-01T00:02:03Z")]
                :res-type #xt/type [:timestamp-tz :second "UTC"]}
               (test-projection '+
                                #(->ts-vec "x" :second (.getEpochSecond (time/->instant #inst "2021")))
                                #(->dur-vec "y" :second 123))))

      (t/is (= {:res [(time/->zdt #inst "2021-01-01T00:00:00.123Z")]
                :res-type #xt/type [:timestamp-tz :milli "UTC"]}
               (test-projection '+
                                #(->ts-vec "x" :second (.getEpochSecond (time/->instant #inst "2021")))
                                #(->dur-vec "y" :milli 123))))

      (t/is (= {:res [(ZonedDateTime/parse "1970-01-01T00:02:34.000001234Z[UTC]")]
                :res-type #xt/type [:timestamp-tz :nano "UTC"]}
               (test-projection '+
                                #(->dur-vec "x" :second 154)
                                #(->ts-vec "y" :nano 1234))))

      (t/is (thrown-with-msg? RuntimeException  #"data exception - overflow error"
                              (test-projection '+
                                               #(->ts-vec "x" :milli (- Long/MAX_VALUE 500))
                                               #(->dur-vec "y" :second 1))))

      (t/is (= {:res [(time/->zdt #inst "2020-12-31T23:59:59.998Z")]
                :res-type #xt/type [:timestamp-tz :micro "UTC"]}
               (test-projection '-
                                #(->ts-vec "x" :micro (time/instant->micros (time/->instant #inst "2021")))
                                #(->dur-vec "y" :milli 2)))))

    (t/is (t/is (= {:res [(Duration/parse "PT23H59M59.999S")]
                    :res-type #xt/type [:duration :milli]}
                   (test-projection '-
                                    #(->ts-vec "x" :milli (.toEpochMilli (time/->instant #inst "2021-01-02")))
                                    #(->ts-vec "y" :milli (.toEpochMilli (time/->instant #inst "2021-01-01T00:00:00.001Z")))))))

    (t/testing "durations"
      (letfn [(->bigint-vec [^String col-name, ^long value]
                (tu/open-vec col-name [value]))

              (->float8-vec [^String col-name, ^double value]
                (tu/open-vec col-name [value]))]

        (t/is (= {:res [(Duration/parse "PT0.002001S")]
                  :res-type #xt/type [:duration :micro]}
                 (test-projection '+
                                  #(->dur-vec "x" :micro 1)
                                  #(->dur-vec "y" :milli 2))))

        (t/is (= {:res [(Duration/parse "PT-1.999S")]
                  :res-type #xt/type [:duration :milli]}
                 (test-projection '-
                                  #(->dur-vec "x" :milli 1)
                                  #(->dur-vec "y" :second 2))))

        (t/is (= {:res [(Duration/parse "PT0.002S")]
                  :res-type #xt/type [:duration :milli]}
                 (test-projection '*
                                  #(->dur-vec "x" :milli 1)
                                  #(->bigint-vec "y" 2))))

        (t/is (= {:res [(Duration/parse "PT10S")]
                  :res-type #xt/type [:duration :second]}
                 (test-projection '*
                                  #(->bigint-vec "x" 2)
                                  #(->dur-vec "y" :second 5))))

        (t/is (= {:res [(Duration/parse "PT0.000012S")]
                  :res-type #xt/type [:duration :micro]}
                 (test-projection '*
                                  #(->float8-vec "x" 2.4)
                                  #(->dur-vec "y" :micro 5))))

        (t/is (= {:res [(Duration/parse "PT3S")]
                  :res-type #xt/type [:duration :second]}
                 (test-projection '/
                                  #(->dur-vec "x" :second 10)
                                  #(->bigint-vec "y" 3))))))))

(t/deftest cast-duration-test
  (letfn [(run-cast [s col-type]
            (with-open [rel (tu/open-rel [{:x s}])]
              (run-projection rel (list 'cast 'x col-type))))]

    (t/is (= [(Duration/ofMillis 1000)]
             (:res (run-cast "1 SECOND" [:duration :milli]))))

    (t/is (= [(Duration/ofNanos 15e8)]
             (:res (run-cast "1.5 SECOND" [:duration :nano]))))

    (t/is (= [(Duration/ofMillis 990)]
             (:res (run-cast "0.99 second" [:duration :micro]))))

    (t/is (= [(Duration/ofMillis (* 2 1000))]
             (:res (run-cast "PT2S" [:duration :micro]))))

    (t/is (= [(Duration/ofMillis (* 1000 60))]
             (:res (run-cast "PT1M" [:duration :milli]))))))

(t/deftest test-struct-literals
  (with-open [rel (tu/open-rel {:x [1.2 3.4]
                                :y [3.4 8.25]})]
    (t/is (= {:res [{:x 1.2, :y 3.4}
                    {:x 3.4, :y 8.25}]
              :res-type #xt/type [:struct {"x" :f64} {"y" :f64}]}
             (run-projection rel '{:x x, :y y})))

    (t/is (= {:res [3.4 8.25], :res-type #xt/type :f64}
             (run-projection rel '(. {:x x, :y y} y))))

    (t/is (= {:res [nil nil], :res-type #xt/type [:? :null]}
             (run-projection rel '(. {:x x, :y y} z))))))

(t/deftest test-namespaced-struct-literals
  ;; everything's normalised by the time it hits the EE now
  (with-open [rel (tu/open-rel {:x$x [1.2 3.4]
                                :y$y [3.4 8.25]})]
    (t/is (= {:res [{:x/x 1.2, :y/y 3.4}
                    {:x/x 3.4, :y/y 8.25}]
              :res-type #xt/type [:struct {"x$x" :f64} {"y$y" :f64}]}
             (run-projection rel '{:x$x x$x, :y$y y$y})))))

(t/deftest test-nested-structs
  (with-open [rel (tu/open-rel {:y [1.2 3.4]})]
    (t/is (= {:res [{:x {:y 1.2}}
                    {:x {:y 3.4}}]
              :res-type #xt/type [:struct {"x" [:struct {"y" :f64}]}]}
             (run-projection rel '{:x {:y y}})))

    (t/is (= {:res [{:y 1.2} {:y 3.4}]
              :res-type #xt/type [:struct {"y" :f64}]}
             (run-projection rel '(. {:x {:y y}} x))))

    (t/is (= {:res [1.2 3.4]
              :res-type #xt/type :f64}
             (run-projection rel '(.. {:x {:y y}} x y))))))

(t/deftest test-struct-equals
  (t/is (= true (project1 '(== {} {}) {})))
  (t/is (= false (project1 '(== {:a 1, :b 2} {:a 1, :b 2, :c 3}) {})))
  (t/is (= false (project1 '(== {:a 1, :b 2, :c 3} {:a 1, :b 2} ) {})))

  (t/is (= true (project1 '(== {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3}) {})))
  (t/is (= false (project1 '(== {:a 1, :b 2, :c 4} {:a 1, :b 2, :c 3}) {})))
  (t/is (= true (project1 '(== {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3.0}) {})))
  (t/is (= false (project1 '(== {:a 1, :b 2, :c 2.5} {:a 1, :b 2, :c 3.0}) {})))

  (t/is (= nil (project1 '(== {:a 1, :b 2, :c nil} {:a 1, :b 2, :c 3.0}) {})))
  (t/is (= false (project1 '(== {:a 1, :b 3, :c nil} {:a 1, :b 2, :c 3.0}) {}))))


(t/deftest test-struct-not-equals
  (t/is (= false (project1 '(<> {} {}) {})))
  (t/is (= true (project1 '(<> {:a 1, :b 2} {:a 1, :b 2, :c 3}) {})))
  (t/is (= true (project1 '(<> {:a 1, :b 2, :c 3} {:a 1, :b 2}) {})))

  (t/is (= false (project1 '(<> {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3}) {})))
  (t/is (= true (project1 '(<> {:a 1, :b 2, :c 4} {:a 1, :b 2, :c 3}) {})))
  (t/is (= false (project1 '(<> {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3.0}) {})))
  (t/is (= true (project1 '(<> {:a 1, :b 2, :c 2.5} {:a 1, :b 2, :c 3.0}) {})))

  (t/is (= nil (project1 '(<> {:a 1, :b 2, :c nil} {:a 1, :b 2, :c 3.0}) {})))
  (t/is (= true (project1 '(<> {:a 1, :b 3, :c nil} {:a 1, :b 2, :c 3.0}) {}))))

(t/deftest test-lists
  (t/testing "simple lists"
    (with-open [rel (tu/open-rel {:x [1.2 3.4]
                                  :y [3.4 8.25]})]
      (t/is (= {:res [[1.2 3.4 10.0]
                      [3.4 8.25 10.0]]
                :res-type #xt/type [:list :f64]}
               (run-projection rel '[x y 10.0])))

      (t/is (= {:res [[1.2 3.4] [3.4 8.25]]
                :res-type #xt/type [:list [:? :f64]]}
               (run-projection rel '[(nth [x y] 0)
                                     (nth [x y] 1)])))))

  (t/testing "nil idxs"
    (with-open [rel (tu/open-rel {:x [1.2 3.4]
                                  :y [0 nil]})]
      (t/is (= {:res [1.2 nil]
                :res-type #xt/type [:? :f64]}
               (run-projection rel '(nth [x] y))))))

  (t/testing "index out of bounds"
    (with-open [rel (tu/open-rel {:x [1.2 3.4]})]
      (t/is (= {:res [nil nil], :res-type #xt/type [:? :f64]}
               (run-projection rel '(nth [x] -1)))))

    (with-open [rel (tu/open-rel {:x [1.2 3.4]})]
      (t/is (= {:res [nil nil], :res-type #xt/type [:? :f64]}
               (run-projection rel '(nth [x] 1))))))

  (t/testing "might not be lists"
    (with-open [rel (tu/open-rel {:x [12.0
                                      [1 2 3]
                                      [4 5]
                                      "foo"]})]
      (t/is (= {:res [nil 2 5 nil]
                :res-type #xt/type [:? :i64]}
               (run-projection rel '(nth x 1))))))

  (t/testing "Nested expr"
    (t/is (= [42] (project1 '[(+ 1 a)] {:a 41})))))

(t/deftest test-list-equal
  (t/is (= true (project1 '(== [] []) {})))
  (t/is (= false (project1 '(== [1 2] [1 2 3]) {})))
  (t/is (= false (project1 '(== [1 2 3] [1 2]) {})))

  (t/is (= true (project1 '(== [1 2 3] [1 2 3]) {})))
  (t/is (= false (project1 '(== [1 2 4] [1 2 3]) {})))
  (t/is (= true (project1 '(== [1 2 3] [1 2 3.0]) {})))
  (t/is (= false (project1 '(== [1 2 2.5] [1 2 3.0]) {})))

  (t/is (= nil (project1 '(== [1 2 nil] [1 2 3.0]) {})))
  (t/is (= false (project1 '(== [1 3 nil] [1 2 3.0]) {})))

  (t/is (= true (project1 '(== [[1 2] [3 4]] [[1 2] [3 4]]) {})))

  (t/testing "strictly-typed list equality"
    (t/is (true? (project1 '(=== [nil] [nil]) {})))
    (t/is (false? (project1 '(=== [1 2 3] [1 2 3.0]) {})))))

(t/deftest test-list-diff
  (t/is (= false (project1 '(<> [] []) {})))
  (t/is (= true (project1 '(<> [1 2] [1 2 3]) {})))
  (t/is (= true (project1 '(<> [1 2 3] [1 2]) {})))

  (t/is (= false (project1 '(<> [1 2 3] [1 2 3]) {})))
  (t/is (= true (project1 '(<> [1 2 4] [1 2 3]) {})))
  (t/is (= false (project1 '(<> [1 2 3] [1 2 3.0]) {})))
  (t/is (= true (project1 '(<> [1 2 2.5] [1 2 3.0]) {})))

  (t/is (= nil (project1 '(<> [1 2 nil] [1 2 3.0]) {})))
  (t/is (= true (project1 '(<> [1 3 nil] [1 2 3.0]) {})))

  (t/is (= false (project1 '(<> [[1 2] [3 4]] [[1 2] [3 4]]) {}))))

(t/deftest test-sets
  (t/is (= #{1 2 3}
           (project1 'a
                     {:a #{1 2 3}})))

  #_ ; TODO `=` on sets
  (t/is (= true
           (project1 '(== #{1 2 3} a)
                     {:a #{1 2 3}})))

  (t/is (= {:roles #{:a :b :c}}
           (project1 '{:roles roles} {:roles #{:a :b :c}})))

  (t/is (= {:roles #{:a :b :c}}
           (project1 '{:roles #{:a role :c}} {:role :b}))))

(t/deftest test-mixing-prims-with-non-prims
  (with-open [rel (tu/open-rel {:x [{:a 42, :b 8}, {:a 12, :b 5}]})]
    (t/is (= {:res [{:a 42, :b 8, :sum 50}
                    {:a 12, :b 5, :sum 17}]
              :res-type #xt/type [:struct {"a" :i64} {"b" :i64} {"sum" :i64}]}
             (run-projection rel '{:a (. x a)
                                   :b (. x b)
                                   :sum (+ (. x a) (. x b))})))))

(t/deftest test-multiple-struct-legs
  (with-open [rel (tu/open-rel {:x [{:a 42}
                                    {:a 12, :b 5}
                                    {:b 10}
                                    {:a nil, :b 12}
                                    {:a 15, :b 25.0}
                                    10.0]})]
    (t/is (= {:res [{:a 42}
                    {:a 12, :b 5}
                    {:b 10}
                    {:b 12}
                    {:a 15, :b 25.0}
                    10.0]
              :res-type #xt/type [:union :f64 [:struct {"a" [:? :i64]} {"b" [:union :f64 [:? :null] [:? :i64]]}]]}
             (run-projection rel 'x)))

    (t/is (= {:res [42 12 nil nil 15 nil]
              :res-type #xt/type [:? :i64]}
             (run-projection rel '(. x a))))

    (t/is (= {:res [{:xa 42}
                    {:xa 12, :xb 5}
                    {:xb 10}
                    {:xb 12}
                    {:xa 15, :xb 25.0}
                    {}],
              :res-type #xt/type [:struct {"xa" [:? :i64]} {"xb" [:union :f64 [:? :null] :i64]}]}
             (run-projection rel '{:xa (. x a),
                                   :xb (. x b)})))))

#_ ; FIXME #2448
(t/deftest test-least-upper-bound-upcast
  ;; when we have nested polymorphic values, two different types may live in the same key of a DUV
  ;; e.g. when we take the union of `[:list :i64]` and `[:list :f64]`, we get `[:list [:union #{:i64 :f64}]]`

  ;; (n.b. we could have opted for `[:union #{[:list :i64] [:list :f64]}]`, which is a stronger type,
  ;;  but this would lead to bigger type explosions)

  (t/is (= [[1] [1.5]]
           (project1 '[[1] [1.5]] {})))

  (t/is (= [[1 nil] [1.5 "foo"]]
           (project1 '[[1 nil] [1.5 "foo"]] {})))

  (with-open [rel (tu/open-rel {:x [true false]})]
    (t/is (= {:res [[1] [1.5]],
              :res-type '[:list [:union #{:i64 :f64}]]}
             (run-projection rel '(if x [1] [1.5])))))

  (with-open [rel (tu/open-rel {:x [{:a 5, :b 1}
                                    {:a "foo", :b 1}
                                    {:a 12.0, :b 5, :c 1}
                                    {:b 1.5}]})]
    (t/is (= {:res [5 "foo" 12.0 nil],
              :res-type '[:union #{:i64 :f64 :utf8 :null}]}
             (run-projection rel '(. x a)))))

  (with-open [rel (tu/open-rel {:x [{:a [5], :b 1}
                                    {:a [12.0], :b 5, :c 1}
                                    {:b 1.5}]})]
    (t/is (= {:res [[5] [12.0] nil],
              :res-type '[:union #{[:list [:union #{:i64 :f64}]] :null}]}
             (run-projection rel '(. x a))))))

(t/deftest test-mixing-composite-types
  (with-open [rel (tu/open-rel {:x [{:a 42}
                                    {:a 12.0, :b 5, :c [1 2 3]}
                                    {:b 10, :c [8 1.5]}
                                    {:a 15, :b 25}
                                    10.0]})]

    (t/is (= {:res [{:a 42, :sums [nil nil]}
                    {:a 12.0, :sums [17.0 7]}
                    {:sums [nil 11.5]}
                    {:a 15, :sums [40 nil]}
                    {:sums [nil nil]}],
              :res-type #xt/type [:struct {"a" [:union :f64 [:? :null] :i64]}
                                          {"sums" [:list [:union :f64 [:? :null] :i64]]}]}
             (run-projection rel '{:a (. x a),
                                   :sums [(+ (. x a) (. x b))
                                          (+ (. x b) (nth (. x c) 1))]})))))

(t/deftest absent-handling-2944
  (with-open [rel (vr/rel-reader [(tu/open-vec #xt/field {"x" [:struct {"maybe-float" [:? :f64]} {"maybe-str" [:? :utf8]}]}
                                               [{}])])]

    (t/is (= {:res [nil], :res-type #xt/type [:? :f64]}
             (run-projection rel '(+ 42.0 (. x maybe-float)))))
    (t/is (= {:res [nil], :res-type #xt/type [:? :utf8]}
             (run-projection rel '(lower (. x maybe-str)))))
    (t/is (= {:res [nil], :res-type #xt/type [:? :utf8]}
             (run-projection rel '(upper (. x maybe-str)))))))

(t/deftest test-current-times-111
  (let [inst (Instant/parse "2022-01-01T01:23:45.678912345Z")
        utc-tz (ZoneId/of "UTC")
        utc-zdt (ZonedDateTime/ofInstant inst utc-tz)
        utc-zdt-micros (-> utc-zdt (.truncatedTo ChronoUnit/MICROS))
        la-tz (ZoneId/of "America/Los_Angeles")
        la-zdt (.withZoneSameInstant utc-zdt la-tz)
        la-zdt-micros (-> la-zdt (.truncatedTo ChronoUnit/MICROS))]
    (binding [expr/*clock* (InstantSource/fixed inst)]
      (letfn [(project-fn [form]
                (run-projection (vr/rel-reader [] 1) form))]
        (binding [expr/*default-tz* utc-tz]
          (t/testing "UTC"
            (t/is (= {:res [utc-zdt-micros]
                      :res-type #xt/type [:timestamp-tz :micro "UTC"]}
                     (project-fn '(current-timestamp)))
                  "current-timestamp")

            (t/is (= {:res [(.toLocalDate utc-zdt-micros)]
                      :res-type #xt/type [:date :day]}
                     (project-fn '(current-date)))
                  "current-date")

            (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                      :res-type #xt/type [:time-local :micro]}
                     (project-fn '(current-time)))
                  "current-time")

            (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                      :res-type #xt/type [:time-local :micro]}
                     (project-fn '(local-time)))
                  "local-time")

            (t/is (= {:res [(.toLocalDateTime utc-zdt-micros)]
                      :res-type #xt/type [:timestamp-local :micro]}
                     (project-fn '(local-timestamp)))
                  "local-timestamp")))

        (binding [expr/*default-tz* la-tz]
          (t/testing "LA"
            (t/is (= {:res [la-zdt-micros]
                      :res-type #xt/type [:timestamp-tz :micro "America/Los_Angeles"]}
                     (run-projection (vr/rel-reader [] 1) '(current-timestamp)))
                  "current-timestamp")

            ;; these two are where we may differ from the spec, due to Type's Date and Time types not supporting a TZ.
            ;; I've opted to return these as UTC to differentiate them from `local-date`, `local-time` and `local-timestamp` below.
            (t/is (= {:res [(.toLocalDate utc-zdt-micros)]
                      :res-type #xt/type [:date :day]}
                     (project-fn '(current-date)))
                  "current-date")

            (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                      :res-type #xt/type [:time-local :micro]}
                     (project-fn '(current-time)))
                  "current-time")

            (t/is (= {:res [(.toLocalDate la-zdt-micros)]
                      :res-type #xt/type [:date :day]}
                     (project-fn '(local-date)))
                  "local-date")

            (t/is (= {:res [(.toLocalTime la-zdt-micros)]
                      :res-type #xt/type [:time-local :micro]}
                     (project-fn '(local-time)))
                  "local-time")

            (t/is (= {:res [(.toLocalDateTime la-zdt-micros)]
                      :res-type #xt/type [:timestamp-local :micro]}
                     (project-fn '(local-timestamp)))
                  "local-timestamp")))

        (binding [expr/*default-tz* la-tz]
          (t/testing "timestamp precision"
            (t/is (= {:res [(-> la-zdt (.minusNanos 45))]
                      :res-type #xt/type [:timestamp-tz :nano "America/Los_Angeles"]}
                     (run-projection (vr/rel-reader [] 1) '(current-timestamp 7)))
                  "current-timestamp")

            (t/is (= {:res [(-> la-zdt-micros (.truncatedTo ChronoUnit/SECONDS) (.toLocalDateTime))]
                      :res-type #xt/type [:timestamp-local :second]}
                     (project-fn '(local-timestamp 0)))
                  "local-timestamp"))

          (t/testing "time precision"
            (t/is (= {:res [(-> utc-zdt (.truncatedTo ChronoUnit/MILLIS) (.toLocalTime))]
                      :res-type #xt/type [:time-local :milli]}
                     (project-fn '(current-time 3)))
                  "current-time")

            (t/is (= {:res [(-> la-zdt (.truncatedTo ChronoUnit/MILLIS) (.minusNanos 8e6) (.toLocalTime))]
                      :res-type #xt/type [:time-local :milli]}
                     (project-fn '(local-time 2)))
                  "local-time"))))))

  (t/deftest test-trim-array
    (t/are [expected expr variables]
        (= expected (project1 expr variables))

      nil '(trim-array nil nil) {}
      nil '(trim-array nil 42) {}
      nil '(trim-array [42] nil) {}

      [] '(trim-array [] 0) {}
      [42] '(trim-array [42] 0) {}
      [] '(trim-array [42] 1) {}
      [42] '(trim-array [42, 43] 1) {}
      [] '(trim-array [42, 43] 2) {}

      [] '(trim-array a 0) {:a []}
      nil '(trim-array a 0) {:a nil}
      [42] '(trim-array a 1) {:a [42, 43]})))

(t/deftest test-trim-array-offset-over-trim-exception
  (t/is (thrown-with-msg? RuntimeException #"Data exception - array element error\." (project1 '(trim-array [1 2 3] 4) {}))))

(t/deftest test-cast-numerics
  (letfn [(test-cast
            ([src tgt-type] (test-cast src tgt-type {}))
            ([src tgt-type opts] (project1 (list 'cast src tgt-type) opts)))]
    (t/is (= nil (test-cast nil :i32)))

    (t/is (= nil (test-cast nil :i64)))
    (t/is (= nil (test-cast nil :i16)))
    (t/is (= nil (test-cast nil :f32)))
    (t/is (= nil (test-cast nil :f64)))

    (t/is (= 42 (test-cast 42 :i32)))

    (t/is (= 42 (test-cast 42.0 :i32)))
    (t/is (= 42 (test-cast 42.0 :i16)))
    (t/is (= 42 (test-cast 42.0 :i64)))

    (t/is (= 42.0 (test-cast 42 :f32)))
    (t/is (= 42.0 (test-cast 42 :f64)))

    (t/is (= 42 (test-cast 'a :i32 {:a 42.0})))

    (t/is (= "42" (test-cast 42 :utf8)))

    (t/is (= (byte 42) (test-cast "42" :i8)))
    (t/is (= (short 42) (test-cast "42" :i16)))
    (t/is (= (int 42) (test-cast "42" :i32)))
    (t/is (= (long 42) (test-cast "42" :i64)))
    (t/is (= (float 42) (test-cast "42" :f32)))
    (t/is (= (double 42) (test-cast "42" :f64)))

    (t/is (= true (test-cast 42 :bool)))
    (t/is (= false (test-cast 0 :bool)))))

(t/deftest test-cast-null
  (letfn [(test-null-cast [tgt-type]
            (let [{:keys [res types]} (tu/query-ra [:project [{'res (list 'cast nil tgt-type)}]
                                                    [:table [{}]]]
                                                   {:with-types? true})]
              {:res (:res (first res))
               :type (get types 'res)}))]
    (let [exp {:res nil, :type #xt/type [:? :null]}]
      (t/is (= exp (test-null-cast :i32)))
      (t/is (= exp (test-null-cast :i64)))
      (t/is (= exp (test-null-cast :utf8)))
      (t/is (= exp (test-null-cast [:timestamp-local :milli]))))))

(t/deftest test-uuids
  (t/is (= true
           (project1 '(== #uuid "00000000-0000-0000-0000-000000000000" a)
                     {:a #uuid "00000000-0000-0000-0000-000000000000"}))))

(t/deftest test-cast-uuids
  (t/is (= #uuid "123e4567-e89b-12d3-a456-426614174000"
           (project1 '(cast "123e4567-e89b-12d3-a456-426614174000" :uuid) {})))

  (t/is (= "123e4567-e89b-12d3-a456-426614174000"
           (project1 '(cast #uuid "123e4567-e89b-12d3-a456-426614174000" :utf8) {})))

  (t/is (= #uuid "123e4567-e89b-12d3-a456-426614174000"
           (project1 '(cast (cast #uuid "123e4567-e89b-12d3-a456-426614174000" :utf8) :uuid) {}))))

(t/deftest test-truthy-if
  (t/is (= :true (project1 '(if 42 :true :false) {})))
  (t/is (= :true (project1 '(if true :true :false) {})))
  (t/is (= :false (project1 '(if false :true :false) {})))
  (t/is (= :false (project1 '(if nil :true :false) {}))))

(t/deftest test-type-mismatch-throws-xtdb-error-3183
  (t/is (anomalous? [:incorrect nil
                     #"\+ not applicable to types i64 and utf8"]
                    (project1 '(+ 1 "2") {}))))

(t/deftest test-kw-fns
  (t/is (nil? (project1 '(namespace :bar) {})))
  (t/is (= "foo" (project1 '(namespace :foo/bar) {})))
  (t/is (= "bar" (project1 '(local-name :bar) {})))
  (t/is (= "bar" (project1 '(local-name :foo/bar) {})))

  (t/is (= :bar (project1 '(keyword "bar") {})))
  (t/is (= :foo/bar (project1 '(keyword "foo/bar") {})))

  (t/is (= ":bar" (project1 '(cast :bar :utf8) {})))
  (t/is (= ":foo/bar" (project1 '(cast :foo/bar :utf8) {}))))

(t/deftest test-str
  (t/is (= "" (project1 '(str) {})))

  (t/is (= "hello" (project1 '(str "hello") {})))
  (t/is (= "42" (project1 '(str 42) {})))

  (t/is (= "hello world" (project1 '(str "hello" " " "world") {})))
  (t/is (= "helloworld" (project1 '(str "hello" nil "world") {})))

  (t/is (= "number: 42.0" (project1 '(str "number: " 42.0) {})))
  (t/is (= "keyword: :foo/bar" (project1 '(str "keyword: " :foo/bar) {}))))

(t/deftest test-patch
  (t/is (= {} (project1 '(_patch {} {}) {})))
  (t/is (= {:a 1} (project1 '(_patch nil {:a 1}) {})))
  (t/is (= {:a 1} (project1 '(_patch {:a 2} {:a 1}) {})))
  (t/is (= {:a 1, :b 2} (project1 '(_patch {:a 2, :b 2} {:a 1}) {})))
  (t/is (= {:a 1, :b 1} (project1 '(_patch {:a 2} {:a 1, :b 1}) {})))
  (t/is (= {:a 2} (project1 '(_patch {:a 2} {}) {})))

  (t/is (= {:a 1} (project1 '(_patch {:a 2.4} {:a 1}) {})))
  (t/is (= {:a 2.4} (project1 '(_patch {:a 1} {:a 2.4}) {})))

  (t/is (= [{:a 2.4}, {:a 1}, {:a "3"}]
           (project '(_patch {:a a1} {:a a2})
                    [{:a1 1.0, :a2 2.4}
                     {:a1 1, :a2 nil}
                     {:a1 3, :a2 "3"}]))))

(t/deftest list-equality-batch-bindings-5047
  (t/is (false? (->> (tu/query-ra '[:project [{ret (== [#xt/instant "1970-01-01T00:00:00Z"]
                                                      [#xt/ldt "1970-01-01T00:00"])}]
                                    [:table [{}]]]
                                  {:default-tz #xt/zone "America/New_York"})
                     first :ret))))

(t/deftest test-type-strict-equality
  (t/is (true? (project1 '(=== 3 3) {})) "int === int")
  (t/is (true? (project1 '(=== 3.0 3.0) {})) "float === float")
  (t/is (false? (project1 '(=== 3 3.0) {})) "int !== float")
  (t/is (true? (project1 '(== 3 3.0) {})) "int == float (cross-type coercion)")
  (t/is (true? (project1 '(=== "foo" "foo") {})))
  (t/is (false? (project1 '(=== "foo" "bar") {})))
  (t/is (true? (project1 '(=== a a) {:a nil})) "null === null")
  (t/is (nil? (project1 '(== a a) {:a nil})) "null == null returns null"))

(t/deftest test-struct-equality
  (t/testing "non-strict equality (==)"
    (t/is (true? (project1 '(== {} {}) {})))
    (t/is (true? (project1 '(== {:a 1, :b 2} {:a 1, :b 2}) {})))
    (t/is (false? (project1 '(== {:a 1, :b 2} {:a 1, :b 3}) {})))
    (t/is (false? (project1 '(== {:a 1} {:a 1, :b 2}) {})))
    (t/is (false? (project1 '(== {:a 1, :b 2} {:a 1}) {})))
    (t/is (true? (project1 '(== {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3.0}) {})) "int == float"))

  (t/testing "strict equality (===)"
    (t/is (true? (project1 '(=== {} {}) {})))
    (t/is (true? (project1 '(=== {:a 1, :b 2} {:a 1, :b 2}) {})))
    (t/is (false? (project1 '(=== {:a 1, :b 2} {:a 1, :b 3}) {})))
    (t/is (false? (project1 '(=== {:a 1} {:a 1, :b 2}) {})))
    (t/is (false? (project1 '(=== {:a 1, :b 2} {:a 1}) {})))
    (t/is (false? (project1 '(=== {:a 1, :b 2, :c 3} {:a 1, :b 2, :c 3.0}) {})) "int !== float")))
