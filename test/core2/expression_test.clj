(ns ^{:clojure.tools.namespace.repl/load false}
    core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.properties :as tcp]
            [clojure.test.check.generators :as tcg]
            [clojure.string :as str]
            [core2.edn :as edn])
  (:import core2.vector.IIndirectVector
           (java.time Clock Duration Instant LocalDate ZonedDateTime ZoneId Period LocalDateTime)
           (java.time.temporal ChronoUnit)
           (org.apache.arrow.vector DurationVector TimeStampVector ValueVector PeriodDuration)
           (org.apache.arrow.vector.types.pojo ArrowType$Duration ArrowType$Timestamp)
           org.apache.arrow.vector.types.TimeUnit
           (core2 StringUtil)
           (java.nio ByteBuffer)))

(t/use-fixtures :each tu/with-allocator)

(defn ->data-vecs []
  [(tu/open-vec "a" (map double (range 1000)))
   (tu/open-vec "b" (map double (range 1000)))
   (tu/open-vec "d" (range 1000))
   (tu/open-vec "e" (map #(format "%04d" %) (range 1000)))])

(defn- open-rel ^core2.vector.IIndirectRelation [vecs]
  (iv/->indirect-rel (map iv/->direct-vec vecs)))

(t/deftest test-simple-projection
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(project [form]
              (with-open [project-col (.project (expr/->expression-projection-spec "c" form {:col-types {'a :f64, 'b :f64, 'd :i64}, :param-types {}})
                                                tu/*allocator* in-rel {})]
                (tu/<-column project-col)))]

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
               (project '(= a d)))
            "predicate")

      (t/is (= (mapv #(Math/sin ^double %) (range 1000))
               (project '(sin a)))
            "math")

      (t/is (= (interleave (map float (range)) (repeat 500 0))
               (project '(if (= 0 (mod a 2)) (/ a 2) 0)))
            "if")

      (t/is (thrown? IllegalArgumentException (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (open-rel (->data-vecs))]
    (letfn [(select-relation [form col-types params]
              (alength (.select (expr/->expression-relation-selector form {:col-types col-types,
                                                                           :param-types (expr/->param-types params)})
                                tu/*allocator* in-rel params)))]

      (t/testing "selector"
        (t/is (= 500 (select-relation '(>= a 500) {'a :f64} {})))
        (t/is (= 500 (select-relation '(>= e "0500") {'e :utf8} {}))))

      (t/testing "parameter"
        (t/is (= 500 (select-relation '(>= a ?a) {'a :f64} {'?a 500})))
        (t/is (= 500 (select-relation '(>= e ?e) {'e :utf8} {'?e "0500"})))))))

(t/deftest nil-selection-doesnt-yield-the-row
  (t/is (= 0
           (-> (.select (expr/->expression-relation-selector '(and true nil) {})
                        tu/*allocator* (iv/->indirect-rel [] 1) {})
               (alength)))))

(t/deftest can-extract-min-max-range-from-expression
  (let [μs-2018 (util/instant->micros (util/->instant #inst "2018"))
        μs-2019 (util/instant->micros (util/->instant #inst "2019"))]
    (letfn [(transpose [[mins maxs]]
              (->> (map vector mins maxs)
                   (zipmap [:sys-end :id :sys-start :row-id :app-time-start :app-time-end])
                   (into {} (remove (comp #{[Long/MIN_VALUE Long/MAX_VALUE]} val)))))]
      (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]
                :app-time-end [(inc μs-2019) Long/MAX_VALUE]}
               (transpose (expr.temp/->temporal-min-max-range
                           {"application_time_start" '(<= application_time_start #inst "2019")
                            "application_time_end" '(> application_time_end #inst "2019")}
                           {}))))

      (t/is (= {:app-time-start [μs-2019 μs-2019]}
               (transpose (expr.temp/->temporal-min-max-range
                           {"application_time_start" '(= application_time_start #inst "2019")}
                           {}))))

      (t/testing "symbol column name"
        (t/is (= {:app-time-start [μs-2019 μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {'application_time_start '(= application_time_start #inst "2019")}
                             {})))))

      (t/testing "conjunction"
        (t/is (= {:app-time-start [Long/MIN_VALUE μs-2019]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"application_time_start" '(and (<= application_time_start #inst "2019")
                                                        (<= application_time_start #inst "2020"))}
                             {})))))

      (t/testing "disjunction not supported"
        (t/is (= {}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"application_time_start" '(or (= application_time_start #inst "2019")
                                                       (= application_time_start #inst "2020"))}
                             {})))))

      (t/testing "parameters"
        (t/is (= {:app-time-start [μs-2018 Long/MAX_VALUE]
                  :app-time-end [Long/MIN_VALUE (dec μs-2018)]
                  :sys-start [Long/MIN_VALUE μs-2019]
                  :sys-end [(inc μs-2019) Long/MAX_VALUE]}
                 (transpose (expr.temp/->temporal-min-max-range
                             {"system_time_start" '(>= ?sys-time system_time_start)
                              "system_time_end" '(< ?sys-time system_time_end)
                              "application_time_start" '(<= ?app-time application_time_start)
                              "application_time_end" '(> ?app-time application_time_end)}
                             {'?sys-time (util/->instant #inst "2019",) '?app-time (util/->instant #inst "2018")}))))))))

(defn project
  "Use to test an expression on some example documents. See also, project1.

  Usage: (project '(+ a b) [{:a 1, :b 2}, {:a 3, :b 4}]) ;; => [3, 7]"
  [expr docs]
  (let [docs (map-indexed #(assoc %2 :id %1) docs)
        lp [:project [{'ret expr}] [:table docs]]]
    (mapv :ret (tu/query-ra lp {}))))

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

(t/deftest test-date-trunc
  (let [test-doc {:id :foo,
                  :date (util/->instant #inst "2021-10-21T12:34:56Z")
                  :zdt (-> (util/->zdt #inst "2021-08-21T12:34:56Z")
                           (.withZoneSameLocal (ZoneId/of "Europe/London")))}]
    (letfn [(simple-trunc [time-unit] (project1 (list 'date-trunc time-unit 'date) test-doc))]

      (t/is (= (util/->zdt #inst "2021-10-21") (simple-trunc "DAY")))
      (t/is (= (util/->zdt #inst "2021-10-21T12:34") (simple-trunc "MINUTE")))
      (t/is (= (util/->zdt #inst "2021-10-01") (simple-trunc "MONTH")))
      (t/is (= (util/->zdt #inst "2021-01-01") (simple-trunc "YEAR"))))

    (t/is (= (-> (util/->zdt #inst "2021-08-21")
                 (.withZoneSameLocal (ZoneId/of "Europe/London")))
             (project1 '(date-trunc "DAY" zdt) test-doc))
          "timezone aware")

    (t/is (= (util/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" date) test-doc)))

    (t/is (= (util/->zdt #inst "2021-10-21") (project1 '(date-trunc "DAY" (date-trunc "MINUTE" date)) test-doc)))

    (t/testing "java.time.LocalDate"
      (let [ld (LocalDate/of 2022 3 29)
            trunc #(project1 (list 'date-trunc % 'date) {:date ld})]
        (t/is (= (LocalDate/of 2022 3 29) (trunc "DAY")))
        (t/is (= (LocalDate/of 2022 3 1) (trunc "MONTH")))
        (t/is (= (LocalDate/of 2022 1 1) (trunc "YEAR")))
        (t/is (= (LocalDate/of 2022 1 1) (project1 '(date-trunc "YEAR" (date-trunc "MONTH" date)) {:date ld})))))))

(t/deftest test-date-extract
  ;; TODO units below minute are not yet implemented for any type
  (letfn [(extract [part date-like] (project1 (list 'extract part 'date) {:date date-like}))
          (extract-all [part date-likes] (project (list 'extract part 'date) (map (partial array-map :date) date-likes)))]
    (t/testing "java.time.Instant"
      (let [inst (util/->instant #inst "2022-03-21T13:44:52.344")]
        (t/is (= 44 (extract "MINUTE" inst)))
        (t/is (= 13 (extract "HOUR" inst)))
        (t/is (= 21 (extract "DAY" inst)))
        (t/is (= 3 (extract "MONTH" inst)))
        (t/is (= 2022 (extract "YEAR" inst)))))

    (t/testing "java.time.ZonedDateTime"
      (let [zdt (-> (util/->zdt #inst "2022-03-21T13:44:52.344")
                    (.withZoneSameLocal (ZoneId/of "Europe/London")))]
        (t/is (= 44 (extract "MINUTE" zdt)))
        (t/is (= 13 (extract "HOUR" zdt)))
        (t/is (= 21 (extract "DAY" zdt)))
        (t/is (= 3 (extract "MONTH" zdt)))
        (t/is (= 2022 (extract "YEAR" zdt)))))

    (t/testing "java.time.LocalDate"
      (let [ld (LocalDate/of 2022 03 21)]
        (t/is (= 0 (extract "MINUTE" ld)))
        (t/is (= 0 (extract "HOUR" ld)))
        (t/is (= 21 (extract "DAY" ld)))
        (t/is (= 3 (extract "MONTH" ld)))
        (t/is (= 2022 (extract "YEAR" ld)))))

    (t/testing "mixed types"
      (let [dates [(util/->instant #inst "2022-03-22T13:44:52.344")
                   (-> (util/->zdt #inst "2021-02-23T21:19:10.692")
                       (.withZoneSameLocal (ZoneId/of "Europe/London")))
                   (LocalDate/of 2020 04 18)]]
        (t/is (= [44 19 0] (extract-all "MINUTE" dates)))
        (t/is (= [13 21 0] (extract-all "HOUR" dates)))
        (t/is (= [22 23 18] (extract-all "DAY" dates)))
        (t/is (= [3 2 4] (extract-all "MONTH" dates)))
        (t/is (= [2022 2021 2020] (extract-all "YEAR" dates)))))))

(defn- run-projection [rel form]
  (let [col-types (->> rel
                       (into {} (map (juxt #(symbol (.getName ^IIndirectVector %))
                                           #(types/field->col-type (.getField (.getVector ^IIndirectVector %)))))))]
    (with-open [out-ivec (.project (expr/->expression-projection-spec "out" form {:col-types col-types, :param-types {}})
                                   tu/*allocator* rel {})]
      {:res (tu/<-column out-ivec)
       :res-type (types/field->col-type (.getField (.getVector out-ivec)))})))

(t/deftest test-nils
  (letfn [(run-test [f xs ys]
            (with-open [rel (open-rel [(tu/open-vec "x" xs)
                                       (tu/open-vec "y" ys)])]
              (-> (run-projection rel (list f 'x 'y))
                  :res)))]

    (t/is (= [3 nil nil nil]
             (run-test '+ [1 1 nil nil] [2 nil 2 nil])))))

(t/deftest test-method-too-large-147
  (letfn [(run-test [form]
            (with-open [rel (open-rel [(tu/open-vec "a" [1 nil 3])
                                       (tu/open-vec "b" [1.2 5.3 nil])
                                       (tu/open-vec "c" [2 nil 8])
                                       (tu/open-vec "d" [3.4 nil 5.3])
                                       (tu/open-vec "e" [8 5 3])])]
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
            (with-open [rel (open-rel [(tu/open-vec "x" [x])
                                       (tu/open-vec "y" [y])
                                       (tu/open-vec "z" [z])])]
              (-> (run-projection rel (list f 'x 'y 'z))
                  :res first)))]

    (t/is (= 6 (run-test '+ 1 2 3)))
    (t/is (= 1 (run-test '- 4 2 1)))
    (t/is (true? (run-test '< 1 2 4)))
    (t/is (false? (run-test '> 4 1 2)))))

(defn- project-mono-value [f-sym val col-type]
  (with-open [rel (open-rel [(tu/open-vec "s" col-type [val])])]
    (-> (run-projection rel (list f-sym 's))
        :res
        first)))

(t/deftest test-character-length
  (letfn [(len [s unit] (project1 (list 'character-length 'a unit) {:a s}))]
    (t/are [s]
      (and (= (.count (.codePoints s)) (len s "CHARACTERS"))
           (= (alength (.getBytes s "utf-8")) (len s "OCTETS")))

      ""
      "a"
      "hello"
      "😀")

    (t/is (= nil (len nil "CHARACTERS")))
    (t/is (= nil (len nil "OCTETS")))))

(tct/defspec character-length-is-equiv-to-code-point-count-prop
  (tcp/for-all [^String s tcg/string]
    (= (.count (.codePoints s)) (project1 '(character-length a "CHARACTERS") {:a s}))))

(tct/defspec character-length-octet-is-equiv-to-byte-count-prop
  (tcp/for-all [^String s tcg/string]
    (= (alength (.getBytes s "utf-8")) (project1 '(character-length a "OCTETS") {:a s}))))

(t/deftest test-octet-length
  (letfn [(len [s vec-type] (project-mono-value 'octet-length s vec-type))]
    (t/are [s] (= (alength (.getBytes s "utf-8")) (len s :utf8) (len s :varbinary))
      ""
      "a"
      "hello"
      "😀")
    (t/is (= nil (len nil :null)))))

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
    (t/are [s expected]
      (= expected (project1 '(trim a b c) {:a s, :b "LEADING", :c "$"}))

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
    (t/are [s expected]
      (= expected (project1 '(trim a b c) {:a s, :b "TRAILING", :c "$"}))

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
      (= expected (project1 '(trim a b c) {:a s, :b "BOTH", :c "$"}))

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
    (t/are [s trim-spec expected]
      (= expected (project1 '(trim a b c) {:a s, :b trim-spec, :c nil}))

      "a" "BOTH" nil
      nil "BOTH" nil

      "a" "LEADING" nil
      nil "LEADING" nil

      "a" "TRAILING" nil
      nil "TRAILING" nil))

  (t/testing "extended char plane trim"
    (t/are [s trim-char expected]
      (= expected (project1 '(trim a b c) {:a s, :b "BOTH", :c trim-char}))
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
      (= (str/trim s) (project1 '(trim a b c) {:a s, :b "BOTH", :c " "}))
      (= (str/triml s) (project1 '(trim a b c) {:a s, :b "LEADING", :c " "}))
      (= (str/trimr s) (project1 '(trim a b c) {:a s, :b "TRAILING", :c " "})))))

(defn- btrim [bin trim-spec trim-octet]
  (some-> (project1 '(trim a b c) {:a (some-> bin byte-array), :b trim-spec, :c (some-> trim-octet vector byte-array)})
          expr/resolve-bytes
          vec))

(t/deftest test-binary-trim
  (t/testing "leading trims of 0"
    (t/are [bin expected]
      (= expected (btrim bin "LEADING" 0))

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
      (= expected (btrim bin "TRAILING" 0))

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
      (= expected (btrim bin "BOTH" 0))

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
    (t/are [bin trim-spec expected]
      (= expected (btrim bin trim-spec nil))

      [42] "BOTH" nil
      nil "BOTH" nil

      [42] "LEADING" nil
      nil "LEADING" nil

      [42] "TRAILING" nil
      nil "TRAILING" nil))

  (t/testing "numeric octet is permitted"
    ;; no defined behaviour for bigint / bigdec
    (t/are [bin trim-spec octet expected]
      (= expected (some-> (project1 '(trim a b c) {:a (some-> bin byte-array), :b trim-spec, :c octet})
                          expr/resolve-bytes
                          vec))

      nil "BOTH" (byte 0) nil
      nil "BOTH" (int 0) nil
      nil "BOTH" (short 0) nil
      nil "BOTH" (long 0) nil
      nil "BOTH" (float 0) nil
      nil "BOTH" (double 0) nil

      nil "LEADING" (byte 0) nil
      nil "LEADING" (int 0) nil
      nil "LEADING" (short 0) nil
      nil "LEADING" (long 0) nil
      nil "LEADING" (float 0) nil
      nil "LEADING" (double 0) nil

      nil "TRAILING" (byte 0) nil
      nil "TRAILING" (int 0) nil
      nil "TRAILING" (short 0) nil
      nil "TRAILING" (long 0) nil
      nil "TRAILING" (float 0) nil
      nil "TRAILING" (double 0) nil

      [0 42 0] "BOTH" (byte 0) [42]
      [0 42 0] "BOTH" (int 0) [42]
      [0 42 0] "BOTH" (short 0) [42]
      [0 42 0] "BOTH" (long 0) [42]
      [0 42 0] "BOTH" (float 0) [42]
      [0 42 0] "BOTH" (double 0) [42]

      [0 42 0] "LEADING" (byte 0) [42 0]
      [0 42 0] "LEADING" (int 0) [42 0]
      [0 42 0] "LEADING" (short 0) [42 0]
      [0 42 0] "LEADING" (long 0) [42 0]
      [0 42 0] "LEADING" (float 0) [42 0]
      [0 42 0] "LEADING" (double 0) [42 0]

      [0 42 0] "TRAILING" (byte 0) [0 42]
      [0 42 0] "TRAILING" (int 0) [0 42]
      [0 42 0] "TRAILING" (short 0) [0 42]
      [0 42 0] "TRAILING" (long 0) [0 42]
      [0 42 0] "TRAILING" (float 0) [0 42]
      [0 42 0] "TRAILING" (double 0) [0 42])))

(tct/defspec bin-trim-is-equiv-to-str-trim-on-utf8-prop
  (tcp/for-all [^String s (tcg/fmap (comp all-whitespace-to-spaces str/join) (tcg/vector (tcg/elements [tcg/string (tcg/return " ")])))]
    (and
      (= (str/trim s)
         (String. (byte-array (btrim (.getBytes s "utf-8") "BOTH" 32)) "utf-8"))
      (= (str/triml s)
         (String. (byte-array (btrim (.getBytes s "utf-8") "LEADING" 32)) "utf-8"))
      (= (str/trimr s)
         (String. (byte-array (btrim (.getBytes s "utf-8") "TRAILING" 32)) "utf-8")))))

(t/deftest test-upper
  (t/are [s expected]
    (= expected (project1 '(upper a) {:a s}))
    nil nil
    "" ""
    " " " "
    "a" "A"
    "aa" "AA"
    "AA" "AA"))

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
  (t/are [s1 s2 unit expected]
    (= expected (project1 (list 'position 'a 'b unit) {:a s1, :b s2}))

    nil nil "CHARACTERS" nil
    nil "" "CHARACTERS" nil
    "" nil "CHARACTERS" nil

    "" "" "CHARACTERS" 1
    "" "" "OCTETS" 1

    "a" "" "CHARACTERS" 0
    "" "a" "CHARACTERS" 1
    "a" "a" "CHARACTERS" 1
    "b" "a" "CHARACTERS" 0

    "a" "" "OCTETS" 0
    "" "a" "OCTETS" 1
    "a" "a" "OCTETS" 1
    "b" "a" "OCTETS" 0

    "😎" "😎" "CHARACTERS" 1
    "😎" "a😎" "CHARACTERS" 2
    "😎" "🌝😎" "CHARACTERS" 2

    "😎" "😎" "OCTETS" 1
    "😎" "a😎" "OCTETS" 2
    "😎" "aa😎" "OCTETS" 3
    "😎" "🌝😎" "OCTETS" 5))

(tct/defspec position-is-codepoint-count-from-idx-prop
  (tcp/for-all [s1 tcg/string
                s2 tcg/string]
    (let [pos (project1 '(position a b "CHARACTERS") {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc (Character/codePointCount (str s1) (int 0) (int i))))
        (zero? pos)))))

(tct/defspec position-is-equiv-to-idx-of-on-ascii-prop
  (tcp/for-all [s1 tcg/string-ascii
                s2 tcg/string-ascii]
    (let [pos (project1 '(position a b "CHARACTERS") {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc i))
        (zero? pos)))))

(tct/defspec position-on-octet-is-equiv-to-idx-of-on-ascii-prop
  (tcp/for-all [s1 tcg/string-ascii
                s2 tcg/string-ascii]
    (let [pos (project1 '(position a b "OCTETS") {:a s2, :b s1})]
      (if-some [i (str/index-of s1 s2)]
        (= pos (inc i))
        (zero? pos)))))

(t/deftest binary-position-test
  (t/are [b1 b2 expected]
    (= expected (project1 (list 'position 'a 'b) {:a (some-> b1 byte-array), :b (some-> b2 byte-array)}))
    nil nil nil
    [] [] 1
    [42] [] 0
    [] [42] 1
    [42] [42] 1
    [43] [42] 0
    [42] [43] 0
    [-44 21] [-32 -44 -21] 0
    [-44 -21] [-32 -44 -21] 2))

(tct/defspec binary-position-equiv-to-octet-position-prop
  (tcp/for-all [^String s1 tcg/string
                ^String s2 tcg/string]
    (= (project1 '(position a b "OCTETS") {:a s1, :b s2})
       (project1 '(position a b) {:a (.getBytes s1 "utf-8"), :b (.getBytes s2 "utf-8")}))))

(t/deftest substring-test
  (t/are [s pos len expected]
    (= expected (project1 '(substring a b c true) {:a s, :b pos, :c len}))

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
  (t/is (thrown-with-msg? IllegalArgumentException #"Negative substring length" (project1 '(substring "" 0 -1 true) {}))))

(t/deftest substring-nils-test
  (doseq [a ["" nil]
          b [1 nil]
          c [1 nil]
          d [true false]
          :when (not (and a b c))]
    (t/is (nil? (project1 '(substring a b c d) {:a a, :b b, :c c, :d d})))))

(defn- utf8len [^String s] (StringUtil/utf8Length (ByteBuffer/wrap (.getBytes s "utf-8"))))

(defn- substring-args-gen [string-gen]
  (tcg/bind string-gen
            (fn [s] (tcg/tuple (tcg/return s)
                               (tcg/choose 1 (inc (utf8len s)))
                               (tcg/choose 0 (utf8len s))))))

(tct/defspec substring-with-no-len-is-equiv-to-remaining-str-prop
  (tcp/for-all [[s i] (substring-args-gen tcg/string)]
    (= (project1 '(substring a b c true) {:a s, :b i, :c (- (utf8len s) (dec i))})
       (project1 '(substring a b -1 false) {:a s, :b i}))))

(tct/defspec substring-is-equiv-to-clj-on-ascii-when-idx-within-bounds-prop
  (tcp/for-all [[s i len] (substring-args-gen tcg/string-ascii)]
    (= (subs s (dec i) (min (+ (dec i) len) (count s)))
       (project1 '(substring a b c true) {:a s, :b i, :c len}))))

(t/deftest bin-substring-test
  (t/are [s pos len expected]
    (= expected (vec (expr/resolve-bytes (project1 '(substring a b c true) {:a (some-> s byte-array), :b pos, :c len}))))

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

(tct/defspec bin-substring-is-equiv-to-substring-on-ascii-prop
  (tcp/for-all [[s i len] (substring-args-gen tcg/string-ascii)]
    (= (vec (expr/resolve-bytes (project1 '(substring a b c true) {:a (.getBytes ^String s "ascii"), :b i, :c len})))
       (vec (.getBytes ^String (project1 '(substring a b c true) {:a s, :b i, :c len}) "ascii")))))

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
  (t/is (thrown-with-msg? IllegalArgumentException #"Negative substring length" (project1 '(overlay "" "" 0 0) {}))))

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
                    (tcg/choose 0 (- (count s) i)))))) )

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
    (= (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d (project1 '(character-length a "CHARACTERS") {:a s2})})
       (project1 '(overlay a b c (default-overlay-length b)) {:a s1, :b s2, :c i}))))

(tct/defspec binary-overlay-len-default-is-len-of-placing-prop
  (tcp/for-all [[s1 s2 i] (overlay-args-gen tcg/bytes)]
    (= (project1 '(overlay a b c d) {:a s1, :b s2, :c i, :d (project1 '(octet-length a) {:a s2})})
       (project1 '(overlay a b c (default-overlay-length b)) {:a s1, :b s2, :c i}))))

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

(t/deftest test-min-max
  (letfn [(run-test [form vecs]
            (with-open [rel (open-rel vecs)]
              (-> (run-projection rel form)
                  :res first)))]
    (t/is (= 9 (run-test '(max x y)
                         [(tu/open-vec "x" [1])
                          (tu/open-vec "y" [9])])))
    (t/is (= 1.0 (run-test '(min x y)
                           [(tu/open-vec "x" [1.0])
                            (tu/open-vec "y" [9.0])])))))

(t/deftest can-return-string-multiple-times
  (with-open [rel (open-rel [(tu/open-vec "x" [1 2 3])])]
    (t/is (= {:res ["foo" "foo" "foo"]
              :res-type :utf8}
             (run-projection rel "foo")))))

(t/deftest test-cond
  (letfn [(run-test [expr xs]
            (with-open [rel (open-rel [(tu/open-vec "x" xs)])]
              (run-projection rel expr)))]

    (t/is (= {:res ["big" "small" "tiny" "tiny"]
              :res-type :utf8}
             (run-test '(cond (> x 100) "big", (> x 10) "small", "tiny")
                       [500 50 5 nil])))

    (t/is (= {:res ["big" "small" nil nil]
              :res-type [:union #{:null :utf8}]}
             (run-test '(cond (> x 100) "big", (> x 10) "small")
                       [500 50 5 nil])))))

(t/deftest test-let
  (with-open [rel (open-rel [(tu/open-vec "x" [1 2 3 nil])])]
    (t/is (= {:res [6 9 12 nil]
              :res-type [:union #{:null :i64}]}
             (run-projection rel '(let [y (* x 2)
                                        y (+ y 3)]
                                    (+ x y)))))))

(t/deftest test-case
  (with-open [rel (open-rel [(tu/open-vec "x" [1 2 3 nil])])]
    (t/is (= {:res ["x=1" "x=2" "none of the above" "none of the above"]
              :res-type :utf8}
             (run-projection rel '(case (* x 2)
                                    2 "x=1"
                                    (+ x 2) "x=2"
                                    "none of the above"))))))

(t/deftest test-coalesce
  (letfn [(run-test [expr]
            (with-open [rel (open-rel [(tu/open-vec "x" ["x" nil nil])
                                       (tu/open-vec "y" ["y" "y" nil])])]
              (run-projection rel expr)))]

    (t/is (= {:res ["x" "y" nil]
              :res-type [:union #{:null :utf8}]}
             (run-test '(coalesce x y))))

    (t/is (= {:res ["x" "lit" "lit"]
              :res-type :utf8}
             (run-test '(coalesce x "lit" y))))

    (t/is (= {:res ["x" "y" "default"]
              :res-type :utf8}
             (run-test '(coalesce x y "default"))))))

(t/deftest test-nullif
  (letfn [(run-test [expr]
            (with-open [rel (open-rel [(tu/open-vec "x" ["x" "y" nil "x"])
                                       (tu/open-vec "y" ["y" "y" nil nil])])]
              (run-projection rel expr)))]

    (t/is (= {:res ["x" nil nil "x"]
              :res-type [:union #{:utf8 :null}]}
             (run-test '(nullif x y))))))

(t/deftest test-mixing-numeric-types
  (letfn [(run-test [f x y]
            (with-open [rel (open-rel [(tu/open-vec "x" [x])
                                       (tu/open-vec "y" [y])])]
              (-> (run-projection rel (list f 'x 'y))
                  (update :res first))))]

    (t/is (= {:res 6, :res-type :i32}
             (run-test '+ (int 4) (int 2))))

    (t/is (= {:res 6, :res-type :i64}
             (run-test '+ (int 2) (long 4))))

    (t/is (= {:res 6, :res-type :i16}
             (run-test '+ (short 2) (short 4))))

    (t/is (= {:res 6.5, :res-type :f32}
             (run-test '+ (byte 2) (float 4.5))))

    (t/is (= {:res 6.5, :res-type :f32}
             (run-test '+ (float 2) (float 4.5))))

    (t/is (= {:res 6.5, :res-type :f64}
             (run-test '+ (float 2) (double 4.5))))

    (t/is (= {:res 6.5, :res-type :f64}
             (run-test '+ (int 2) (double 4.5))))

    (t/is (= {:res -2, :res-type :i32}
             (run-test '- (short 2) (int 4))))

    (t/is (= {:res 8, :res-type :i16}
             (run-test '* (byte 2) (short 4))))

    (t/is (= {:res 2, :res-type :i16}
             (run-test '/ (short 4) (byte 2))))

    (t/is (= {:res 2.0, :res-type :f32}
             (run-test '/ (float 4) (int 2))))))

(t/deftest test-throws-on-overflow
  (letfn [(run-unary-test [f x]
            (with-open [rel (open-rel [(tu/open-vec "x" [x])])]
              (-> (run-projection rel (list f 'x))
                  (update :res first))))

          (run-binary-test [f x y]
            (with-open [rel (open-rel [(tu/open-vec "x" [x])
                                       (tu/open-vec "y" [y])])]
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
            :res-type [:union #{:i64 :f64}]}
           (with-open [rel (open-rel [(tu/open-vec "x" [1.2 1 3.4])])]
             (run-projection rel 'x))))

  (t/is (= {:res [4.4 9.75]
            :res-type [:union #{:f32 :f64}]}
           (with-open [rel (open-rel [(tu/open-vec "x" [1 1.5])
                                      (tu/open-vec "y" [3.4 (float 8.25)])])]
             (run-projection rel '(+ x y)))))

  (t/is (= {:res [(float 4.4) nil nil nil]
            :res-type [:union #{:null :f32 :f64}]}
           (with-open [rel (open-rel [(tu/open-vec "x" [1 12 nil nil])
                                      (tu/open-vec "y" [(float 3.4) nil 4.8 nil])])]
             (run-projection rel '(+ x y))))))

(t/deftest test-ternary-booleans
  (t/is (= [{:res [true false nil false false false nil false nil]
             :res-type [:union #{:null :bool}]}
            {:res [true true true true false nil true nil nil]
             :res-type [:union #{:null :bool}]}]
           (with-open [rel (open-rel [(tu/open-vec "x" [true true true false false false nil nil nil])
                                      (tu/open-vec "y" [true false nil true false nil true false nil])])]
             [(run-projection rel '(and x y))
              (run-projection rel '(or x y))])))

  (t/is (= [{:res [false true nil]
             :res-type [:union #{:null :bool}]}
            {:res [true false false]
             :res-type :bool}
            {:res [false true false]
             :res-type :bool}
            {:res [false false true]
             :res-type :bool}]
           (with-open [rel (open-rel [(tu/open-vec "x" [true false nil])])]
             [(run-projection rel '(not x))
              (run-projection rel '(true? x))
              (run-projection rel '(false? x))
              (run-projection rel '(nil? x))]))))

(t/deftest test-mixing-timestamp-types
  (letfn [(->ts-vec [col-name time-unit, ^long value]
            (doto ^TimeStampVector (.createVector (types/->field col-name (ArrowType$Timestamp. time-unit "UTC") false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (->dur-vec [col-name ^TimeUnit time-unit, ^long value]
            (doto (DurationVector. (types/->field col-name (ArrowType$Duration. time-unit) false) tu/*allocator*)
              (.setValueCount 1)
              (.set 0 value)))

          (test-projection [f-sym ->x-vec ->y-vec]
            (with-open [^ValueVector x-vec (->x-vec)
                        ^ValueVector y-vec (->y-vec)]
              (run-projection (iv/->indirect-rel [(iv/->direct-vec x-vec)
                                                  (iv/->direct-vec y-vec)])
                              (list f-sym 'x 'y))))]

    (t/testing "ts/dur"
      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:02:03Z")]
                :res-type [:timestamp-tz :second "UTC"]}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/SECOND 123))))

      (t/is (= {:res [(util/->zdt #inst "2021-01-01T00:00:00.123Z")]
                :res-type [:timestamp-tz :milli "UTC"]}
               (test-projection '+
                                #(->ts-vec "x" TimeUnit/SECOND (.getEpochSecond (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 123))))

      (t/is (= {:res [(ZonedDateTime/parse "1970-01-01T00:02:34.000001234Z[UTC]")]
                :res-type [:timestamp-tz :nano "UTC"]}
               (test-projection '+
                                #(->dur-vec "x" TimeUnit/SECOND 154)
                                #(->ts-vec "y" TimeUnit/NANOSECOND 1234))))

      (t/is (thrown? ArithmeticException
                     (test-projection '+
                                      #(->ts-vec "x" TimeUnit/MILLISECOND (- Long/MAX_VALUE 500))
                                      #(->dur-vec "y" TimeUnit/SECOND 1))))

      (t/is (= {:res [(util/->zdt #inst "2020-12-31T23:59:59.998Z")]
                :res-type [:timestamp-tz :micro "UTC"]}
               (test-projection '-
                                #(->ts-vec "x" TimeUnit/MICROSECOND (util/instant->micros (util/->instant #inst "2021")))
                                #(->dur-vec "y" TimeUnit/MILLISECOND 2)))))

    (t/is (t/is (= {:res [(Duration/parse "PT23H59M59.999S")]
                    :res-type [:duration :milli]}
                   (test-projection '-
                                    #(->ts-vec "x" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-02")))
                                    #(->ts-vec "y" TimeUnit/MILLISECOND (.toEpochMilli (util/->instant #inst "2021-01-01T00:00:00.001Z")))))))

    (t/testing "durations"
      (letfn [(->bigint-vec [^String col-name, ^long value]
                (tu/open-vec col-name [value]))

              (->float8-vec [^String col-name, ^double value]
                (tu/open-vec col-name [value]))]

        (t/is (= {:res [(Duration/parse "PT0.002001S")]
                  :res-type [:duration :micro]}
                 (test-projection '+
                                  #(->dur-vec "x" TimeUnit/MICROSECOND 1)
                                  #(->dur-vec "y" TimeUnit/MILLISECOND 2))))

        (t/is (= {:res [(Duration/parse "PT-1.999S")]
                  :res-type [:duration :milli]}
                 (test-projection '-
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->dur-vec "y" TimeUnit/SECOND 2))))

        (t/is (= {:res [(Duration/parse "PT0.002S")]
                  :res-type [:duration :milli]}
                 (test-projection '*
                                  #(->dur-vec "x" TimeUnit/MILLISECOND 1)
                                  #(->bigint-vec "y" 2))))

        (t/is (= {:res [(Duration/parse "PT10S")]
                  :res-type [:duration :second]}
                 (test-projection '*
                                  #(->bigint-vec "x" 2)
                                  #(->dur-vec "y" TimeUnit/SECOND 5))))

        (t/is (= {:res [(Duration/parse "PT0.000012S")]
                  :res-type [:duration :micro]}
                 (test-projection '*
                                  #(->float8-vec "x" 2.4)
                                  #(->dur-vec "y" TimeUnit/MICROSECOND 5))))

        (t/is (= {:res [(Duration/parse "PT3S")]
                  :res-type [:duration :second]}
                 (test-projection '/
                                  #(->dur-vec "x" TimeUnit/SECOND 10)
                                  #(->bigint-vec "y" 3))))))))

(t/deftest test-struct-literals
  (with-open [rel (open-rel [(tu/open-vec "x" [1.2 3.4])
                             (tu/open-vec "y" [3.4 8.25])])]
    (t/is (= {:res [{:x 1.2, :y 3.4}
                    {:x 3.4, :y 8.25}]
              :res-type [:struct '{x :f64, y :f64}]}
             (run-projection rel '{:x x, :y y})))

    (t/is (= {:res [3.4 8.25], :res-type [:union #{:null :f64}]}
             (run-projection rel '(. {:x x, :y y} y))))

    (t/is (= {:res [nil nil], :res-type :null}
             (run-projection rel '(. {:x x, :y y} z))))))

(t/deftest test-nested-structs
  (with-open [rel (open-rel [(tu/open-vec "y" [1.2 3.4])])]
    (t/is (= {:res [{:x {:y 1.2}}
                    {:x {:y 3.4}}]
              :res-type [:struct '{x [:struct {y :f64}]}]}
             (run-projection rel '{:x {:y y}})))

    (t/is (= {:res [{:y 1.2} {:y 3.4}]
              :res-type [:union #{:null [:struct '{y :f64}]}]}
             (run-projection rel '(. {:x {:y y}} x))))

    (t/is (= {:res [1.2 3.4]
              :res-type [:union #{:null :f64}]}
             (run-projection rel '(.. {:x {:y y}} x y))))))

(t/deftest test-lists
  (t/testing "simple lists"
    (with-open [rel (open-rel [(tu/open-vec "x" [1.2 3.4])
                               (tu/open-vec "y" [3.4 8.25])])]
      (t/is (= {:res [[1.2 3.4 10.0]
                      [3.4 8.25 10.0]]
                :res-type [:fixed-size-list 3 :f64]}
               (run-projection rel '[x y 10.0])))

      (t/is (= {:res [[1.2 3.4] [3.4 8.25]]
                :res-type [:fixed-size-list 2 [:union #{:null :f64}]]}
               (run-projection rel '[(nth [x y] 0)
                                     (nth [x y] 1)])))))

  (t/testing "nil idxs"
    (with-open [rel (open-rel [(tu/open-vec "x" [1.2 3.4])
                               (tu/open-vec "y" [0 nil])])]
      (t/is (= {:res [1.2 nil]
                :res-type [:union #{:f64 :null}]}
               (run-projection rel '(nth [x] y))))))

  (t/testing "IOOBE"
    (with-open [rel (open-rel [(tu/open-vec "x" [1.2 3.4])])]
      (t/is (thrown? IndexOutOfBoundsException
                     (run-projection rel '(nth [x] -1)))))

    (with-open [rel (open-rel [(tu/open-vec "x" [1.2 3.4])])]
      (t/is (thrown? IndexOutOfBoundsException
                     (run-projection rel '(nth [x] 1))))))

  (t/testing "might not be lists"
    (with-open [rel (open-rel [(tu/open-vec "x"
                                            [12.0
                                             [1 2 3]
                                             [4 5]
                                             "foo"])])]
      (t/is (= {:res [nil 2 5 nil]
                :res-type [:union #{:i64 :null}]}
               (run-projection rel '(nth x 1))))))

  (t/testing "Nested expr"
    (t/is (= [42] (project1 '[(+ 1 a)] {:a 41})))))

(t/deftest test-mixing-prims-with-non-prims
  (with-open [rel (open-rel [(tu/open-vec "x" [{:a 42, :b 8}, {:a 12, :b 5}])])]
    (t/is (= {:res [{:a 42, :b 8, :sum 50}
                    {:a 12, :b 5, :sum 17}]
              :res-type [:struct '{a [:union #{:null :i64}], b [:union #{:null :i64}] sum [:union #{:null :i64}]}]}
             (run-projection rel '{:a (. x a)
                                   :b (. x b)
                                   :sum (+ (. x a) (. x b))})))))

(t/deftest test-multiple-struct-legs
  (with-open [rel (open-rel [(tu/open-vec "x"
                                          [{:a 42}
                                           {:a 12, :b 5}
                                           {:b 10}
                                           {:a 15, :b 25}
                                           10.0])])]
    (t/is (= {:res [{:a 42}
                    {:a 12, :b 5}
                    {:b 10}
                    {:a 15, :b 25}
                    10.0]
              :res-type [:union #{[:struct '{a :i64}]
                                  [:struct '{a :i64, b :i64}]
                                  [:struct '{b :i64}]
                                  :f64}]}
             (run-projection rel 'x)))

    (t/is (= {:res [42 12 nil 15 nil]
              :res-type [:union #{:i64 :null}]}
             (run-projection rel '(. x a))))))

(t/deftest test-current-times-111
  (let [inst (Instant/parse "2022-01-01T01:23:45.678912345Z")
        utc-tz (ZoneId/of "UTC")
        utc-zdt (ZonedDateTime/ofInstant inst utc-tz)
        utc-zdt-micros (-> utc-zdt (.truncatedTo ChronoUnit/MICROS))
        la-tz (ZoneId/of "America/Los_Angeles")
        la-zdt (.withZoneSameInstant utc-zdt la-tz)
        la-zdt-micros (-> la-zdt (.truncatedTo ChronoUnit/MICROS))]
    (letfn [(project-fn [form]
              (run-projection (iv/->indirect-rel [] 1) form))]
      (binding [expr/*clock* (Clock/fixed inst utc-tz)]
        (t/testing "UTC"
          (t/is (= {:res [utc-zdt-micros]
                    :res-type [:timestamp-tz :micro "UTC"]}
                   (project-fn '(current-timestamp)))
                "current-timestamp")

          (t/is (= {:res [(.toLocalDate utc-zdt-micros)]
                    :res-type [:date :day]}
                   (project-fn '(current-date)))
                "current-date")

          (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                    :res-type [:time-local :micro]}
                   (project-fn '(current-time)))
                "current-time")

          (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                    :res-type [:time-local :micro]}
                   (project-fn '(local-time)))
                "local-time")

          (t/is (= {:res [(.toLocalDateTime utc-zdt-micros)]
                    :res-type [:timestamp-local :micro]}
                   (project-fn '(local-timestamp)))
                "local-timestamp")))

      (binding [expr/*clock* (Clock/fixed inst la-tz)]
        (t/testing "LA"
          (t/is (= {:res [la-zdt-micros]
                    :res-type [:timestamp-tz :micro "America/Los_Angeles"]}
                   (run-projection (iv/->indirect-rel [] 1) '(current-timestamp)))
                "current-timestamp")

          ;; these two are where we may differ from the spec, due to Arrow's Date and Time types not supporting a TZ.
          ;; I've opted to return these as UTC to differentiate them from `local-time` and `local-timestamp` below.
          (t/is (= {:res [(.toLocalDate utc-zdt-micros)]
                    :res-type [:date :day]}
                   (project-fn '(current-date)))
                "current-date")

          (t/is (= {:res [(.toLocalTime utc-zdt-micros)]
                    :res-type [:time-local :micro]}
                   (project-fn '(current-time)))
                "current-time")

          (t/is (= {:res [(.toLocalTime la-zdt-micros)]
                    :res-type [:time-local :micro]}
                   (project-fn '(local-time)))
                "local-time")

          (t/is (= {:res [(.toLocalDateTime la-zdt-micros)]
                    :res-type [:timestamp-local :micro]}
                   (project-fn '(local-timestamp)))
                "local-timestamp")))

      (binding [expr/*clock* (Clock/fixed inst, (ZoneId/of "America/Los_Angeles"))]
        (t/testing "timestamp precision"
          (t/is (= {:res [(-> la-zdt (.minusNanos 45))]
                    :res-type [:timestamp-tz :nano "America/Los_Angeles"]}
                   (run-projection (iv/->indirect-rel [] 1) '(current-timestamp 7)))
                "current-timestamp")

          (t/is (= {:res [(-> la-zdt-micros (.truncatedTo ChronoUnit/SECONDS) (.toLocalDateTime))]
                    :res-type [:timestamp-local :second]}
                   (project-fn '(local-timestamp 0)))
                "local-timestamp"))

        (t/testing "time precision"
          (t/is (= {:res [(-> utc-zdt (.truncatedTo ChronoUnit/MILLIS) (.toLocalTime))]
                    :res-type [:time-local :milli]}
                   (project-fn '(current-time 3)))
                "current-time")

          (t/is (= {:res [(-> la-zdt (.truncatedTo ChronoUnit/MILLIS) (.minusNanos 8e6) (.toLocalTime))]
                    :res-type [:time-local :milli]}
                   (project-fn '(local-time 2)))
                "local-time"))))))

(t/deftest test-interval-constructors
  (t/are [expected expr data]
    (= (some-> expected edn/period-duration-reader) (project1 expr data))

    nil '(single-field-interval nil "YEAR" 2 0) {}
    nil '(single-field-interval nil "MONTH" 2 0) {}
    nil '(single-field-interval nil "DAY" 2 0) {}
    nil '(single-field-interval nil "HOUR" 2 0) {}
    nil '(single-field-interval nil "MINUTE" 2 0) {}
    nil '(single-field-interval nil "SECOND" 2 6) {}

    "P0D PT0S" '(single-field-interval 0 "YEAR" 2 0) {}
    "P0D PT0S" '(single-field-interval 0 "MONTH" 2 0) {}
    "P0D PT0S" '(single-field-interval 0 "DAY" 2 0) {}
    "P0D PT0S" '(single-field-interval 0 "HOUR" 2 0) {}
    "P0D PT0S" '(single-field-interval 0 "MINUTE" 2 0) {}
    "P0D PT0S" '(single-field-interval 0 "SECOND" 2 0) {}

    "P0D PT0S" '(single-field-interval a "YEAR" 2 0) {:a 0}
    "P0D PT0S" '(single-field-interval a "MONTH" 2 0) {:a 0}
    "P0D PT0S" '(single-field-interval a "DAY" 2 0) {:a 0}
    "P0D PT0S" '(single-field-interval a "HOUR" 2 0) {:a 0}
    "P0D PT0S" '(single-field-interval a "MINUTE" 2 0) {:a 0}
    "P0D PT0S" '(single-field-interval a "SECOND" 2 0) {:a 0}

    ;; Y / M distinction is lost when writing to IntervalYear vectors
    "P12M PT0S" '(single-field-interval 1 "YEAR" 2 0) {}
    "P-24M PT0S" '(single-field-interval -2 "YEAR" 2 0) {}

    "P1M PT0S" '(single-field-interval 1 "MONTH" 2 0) {}
    "P-2M PT0S" '(single-field-interval -2 "MONTH" 2 0) {}

    "P1D PT0S" '(single-field-interval 1 "DAY" 2 0) {}
    "P-2D PT0S" '(single-field-interval -2 "DAY" 2 0) {}

    "P0D PT1H" '(single-field-interval 1 "HOUR" 2 0) {}
    "P0D PT-2H" '(single-field-interval -2 "HOUR" 2 0) {}

    "P0D PT1M" '(single-field-interval 1 "MINUTE" 2 0) {}
    "P0D PT-2M" '(single-field-interval -2 "MINUTE" 2 0) {}

    "P0D PT1S" '(single-field-interval 1 "SECOND" 2 6) {}
    "P0D PT-2S" '(single-field-interval -2 "SECOND" 2 6) {}

    ;; fractional seconds
    "P0D PT1.34S" '(single-field-interval "1.34" "SECOND" 2 6) {}

    ;; multi part parsing
    nil '(multi-field-interval nil "YEAR" 2 "MONTH" 2) {}

    "P0D PT0S" '(multi-field-interval "0-0" "YEAR" 2 "MONTH" 2) {}
    "P12M PT0S" '(multi-field-interval "1-0" "YEAR" 2 "MONTH" 2) {}
    "P12M PT0S" '(multi-field-interval "+1-0" "YEAR" 2 "MONTH" 2) {}
    "P-12M PT0S" '(multi-field-interval "-1-0" "YEAR" 2 "MONTH" 2) {}
    "P13M PT0S" '(multi-field-interval "1-1" "YEAR" 2 "MONTH" 2) {}

    "P11D PT12H" '(multi-field-interval "11 12" "DAY" 2 "HOUR" 2) {}
    "P-1D PT-1S" '(multi-field-interval "-1 00:00:01" "DAY" 2 "SECOND" 6) {}
    "P1D PT2M" '(multi-field-interval "1 00:02" "DAY" 2 "MINUTE" 2) {}
    "P1D PT23H" '(multi-field-interval "1 23" "DAY" 2 "HOUR" 2) {}

    "P0D PT-3H-4M-1S" '(multi-field-interval "-03:04:01" "HOUR" 2 "SECOND" 6) {}
    "P0D PT23H2M" '(multi-field-interval "23:02" "HOUR" 2 "MINUTE" 2) {}

    "P0D PT44M34S" '(multi-field-interval "44:34" "MINUTE" 2 "SECOND" 6) {}
    "P0D PT44M34.123456S" '(multi-field-interval "44:34.123456" "MINUTE" 2 "SECOND" 6) {}

    "P1D PT1.334S" '(multi-field-interval "1 00:00:01.334" "DAY" 2 "SECOND" 6) {}
    "P0D PT3H4M1.334S" '(multi-field-interval "03:04:1.334" "HOUR" 2 "SECOND" 6) {}
    "P0D PT44M34.123456789S" '(multi-field-interval "44:34.123456789" "MINUTE" 2 "SECOND" 6) {}

    ;; truncates when we can no longer represent the number
    "P0D PT44M34.123456789S" '(multi-field-interval "44:34.123456789666" "MINUTE" 2 "SECOND" 6) {}

    "P0D PT0.123S" '(multi-field-interval "+00:00.123" "MINUTE" 2 "SECOND" 6) {}
    "P0D PT0.123S" '(multi-field-interval "00:00.123" "MINUTE" 2 "SECOND" 6) {}
    "P0D PT-0.123S" '(multi-field-interval "-00:00.123" "MINUTE" 2 "SECOND" 6) {}))

(t/deftest test-multi-part-interval-ex-cases
  (letfn [(p [unit1 unit2] (project1 (list 'multi-field-interval "0-0" unit1 2 unit2 2) {}))]
    (t/is (thrown-with-msg? IllegalArgumentException #"If YEAR specified as the interval start field, MONTH must be the end field\." (p "YEAR" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"MONTH is not permitted as the interval start field\." (p "MONTH" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"Interval end field must have less significance than the start field\." (p "DAY" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"Interval end field must have less significance than the start field\." (p "MINUTE" "HOUR")))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec single-field-interval-string-parse-same-as-int-prop
  (tcp/for-all [i (tcg/choose -99 99)
                unit (tcg/elements ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
                include-plus tcg/boolean]
    (= (project1 (list 'single-field-interval i unit 2 (if (= "SECOND" unit) 6 0)) {})
       (project1 (list 'single-field-interval
                       (cond (neg? i) (str i)
                             include-plus (str "+" i)
                             :else (str i))
                       unit
                       2
                       (if (= "SECOND" unit) 6 0))
                 {}))))

(t/deftest test-interval-arithmetic
  (t/are [expected expr]
    (= (some-> expected edn/period-duration-reader) (project1 expr {}))

    nil '(+ (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(+ nil (single-field-interval 1 "YEAR" 2 0))

    nil '(- (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(- nil (single-field-interval 1 "YEAR" 2 0))

    nil '(* (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(* nil (single-field-interval 1 "YEAR" 2 0))

    "P24M PT0S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0))
    "P13M PT0S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
    "P11M PT0S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "MONTH" 2 0))

    "P12M-1D PT0S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "DAY" 2 0))
    "P12M PT-1S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "SECOND" 2 6))
    "P12M PT1H1S" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "HOUR" 2 0) (single-field-interval 1 "SECOND" 2 6))

    "P0D PT0S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0))
    "P11M PT0S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
    "P13M PT0S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "MONTH" 2 0))

    "P12M1D PT0S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "DAY" 2 0))
    "P12M PT1S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "SECOND" 2 6))
    "P12M PT-1H-1S" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "HOUR" 2 0) (single-field-interval 1 "SECOND" 2 6))

    "P36M PT0S" '(* (single-field-interval 1 "YEAR" 2 0) 3)

    "P6M PT0S" '(/ (single-field-interval 1 "YEAR" 2 0) 2)
    "P2M PT0S" '(/ (single-field-interval 1 "YEAR" 2 0) 5)

    "P12M PT0S" '(/ (+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0)) 2)

    "P0M PT0S" '(/ (single-field-interval 1 "MONTH" 2 0) 2)
    "P1M PT0S" '(/ (single-field-interval 6 "MONTH" 2 0) 5)

    "P0M PT0S" '(/ (single-field-interval 1 "DAY" 2 0) 2)
    "P1D PT0S" '(/ (single-field-interval 6 "DAY" 2 0) 5)))

(t/deftest test-uoe-thrown-for-unsupported-div
  (t/is (thrown? UnsupportedOperationException (project1 '(/ (+ (single-field-interval 1 "MONTH" 2 0) (single-field-interval 3 "MINUTE" 2 0)) 3) {})))
  (t/is (thrown? UnsupportedOperationException (project1 '(/ (+ (single-field-interval 1 "MONTH" 2 0) (single-field-interval 3 "DAY" 2 0)) 3) {}))))

(def period-gen
  (tcg/fmap (fn [[y m d]]
              (Period/of y m d))
            (tcg/tuple
              (tcg/return 0)
              tcg/small-integer
              tcg/small-integer)))

(def small-duration-gen
  "Generates java.time.Duration instances representing -1D to +1D (exclusive), this is useful
  as the fractional component of MonthDayNano is going to hold nanos in this range."
  (tcg/fmap #(Duration/ofNanos %) (tcg/choose -86399999999999 86399999999999)))

(def period-duration-gen
  (tcg/fmap
    #(PeriodDuration. (first %) (second %))
    (tcg/tuple period-gen small-duration-gen)))

;; some basic interval algebraic properties

(def pd-zero (PeriodDuration. Period/ZERO Duration/ZERO))

(tct/defspec interval-add-identity-prop
  (tcp/for-all [pd period-duration-gen]
    (= pd (project1 '(+ a b) {:a pd, :b pd-zero}))))

(tct/defspec interval-sub-identity-prop
  (tcp/for-all [pd period-duration-gen]
    (= pd (project1 '(- a b) {:a pd, :b pd-zero}))))

(tct/defspec interval-mul-factor-identity-prop
  (tcp/for-all [pd period-duration-gen]
    (= pd (project1 '(* a 1) {:a pd}))))

(tct/defspec interval-mul-by-zero-prop
  (tcp/for-all [pd period-duration-gen]
    (= #time/period-duration "P0M PT0S" (project1 '(* a 0) {:a pd}))))

(tct/defspec interval-add-sub-round-trip-prop
  (tcp/for-all [pd period-duration-gen]
    (= pd (project1 '(- (+ a a) a) {:a pd}))))

(tct/defspec interval-mul-by-2-is-equiv-to-sum-self-prop
  (tcp/for-all [pd period-duration-gen]
    (= (project1 '(* a 2) {:a pd})
       (project1 '(+ a a) {:a pd}))))

(tct/defspec interval-mul-by-neg1-is-equiv-to-sub-self2-prop
  (tcp/for-all [pd period-duration-gen]
    (= (project1 '(* a -1) {:a pd})
       (project1 '(- a a a) {:a pd}))))

(t/deftest test-interval-abs
  (t/are [expected expr]
    (= (some-> expected edn/period-duration-reader) (project1 expr {}))

    "P0D PT0S" '(abs (single-field-interval 0 "YEAR" 2 0))
    "P12M PT0S" '(abs (single-field-interval 1 "YEAR" 2 0))
    "P12M PT0S" '(abs (single-field-interval -1 "YEAR" 2 0))

    "P11M PT0S" '(abs (+ (single-field-interval -1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0)))
    "P1D PT-1S" '(abs (+ (single-field-interval -1 "DAY" 2 0) (single-field-interval 1 "SECOND" 2 6)))))

(t/deftest test-interval-equality-quirks
  (t/are [expr expected]
    (= expected (project1 expr {}))

    '(= (single-field-interval 0 "YEAR" 2 0)) true
    '(= (single-field-interval 0 "YEAR" 2 0) (single-field-interval 0 "DAY" 2 0)) true
    '(= (single-field-interval 0 "YEAR" 2 0) (single-field-interval 1 "DAY" 2 0)) false

    '(= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0)) true))

(def single-interval-constructor-gen
  (->> (tcg/hash-map
         :unit (tcg/elements ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
         :sign (tcg/elements [nil "-" "+"])
         :force-string tcg/boolean
         :leading-value (tcg/choose 0 999999999)
         :fractional-value (tcg/choose 0 99999999)
         :use-fractional-value tcg/boolean
         :precision (tcg/choose 1 8)
         :use-precision tcg/boolean
         :fractional-precision (tcg/choose 0 9)
         :use-fractional-precision tcg/boolean)
       (tcg/fmap
         (fn [{:keys [unit
                      sign
                      force-string
                      leading-value
                      fractional-value
                      use-fractional-value
                      precision
                      use-precision
                      fractional-precision
                      use-fractional-precision]}]

           (let [precision (if use-precision precision 2)

                 fractional-precision
                 (cond
                   (not= "SECOND" unit) 0
                   use-fractional-precision fractional-precision
                   :else 2)

                 leading-value (mod leading-value (parse-long (str/join (repeat precision "9"))))

                 fractional-value
                 (if (pos? fractional-precision)
                   (mod fractional-value (parse-long (str/join (repeat fractional-precision "9"))))
                   0)

                 use-fractional-value (and (= "SECOND" unit) use-fractional-value)

                 v
                 (cond
                   force-string (str sign leading-value (when use-fractional-value (str "." fractional-value)))
                   use-fractional-value (str sign leading-value (when use-fractional-value (str "." fractional-value)))
                   :else leading-value)]

             (list 'single-field-interval v unit precision fractional-precision))))))

(def multi-interval-fields-gen
  (tcg/bind
    (tcg/elements ["YEAR" "DAY" "HOUR" "MINUTE"])
    (fn [start]
      (tcg/tuple (tcg/return start)
                 (case start
                   "YEAR" (tcg/return "MONTH")
                   "DAY" (tcg/elements ["HOUR" "MINUTE" "SECOND"])
                   "HOUR" (tcg/elements ["MINUTE" "SECOND"])
                   "MINUTE" (tcg/return "SECOND"))))))

(def multi-interval-constructor-gen
  (->> (tcg/hash-map
         :fields multi-interval-fields-gen
         :sign (tcg/elements [nil "-" "+"])
         :time-values (tcg/hash-map
                        "YEAR" (tcg/choose 0 999999999)
                        "MONTH" (tcg/choose 0 999999999)
                        "DAY" (tcg/choose 0 999999999)
                        "HOUR" (tcg/choose 0 23)
                        "MINUTE" (tcg/choose 0 59)
                        "SECOND" (tcg/choose 0 59))
         :fractional-value (tcg/choose 0 99999999)
         :use-fractional-value tcg/boolean
         :precision (tcg/choose 1 8)
         :use-precision tcg/boolean
         :fractional-precision (tcg/choose 0 9)
         :use-fractional-precision tcg/boolean)
       (tcg/fmap
         (fn [{:keys [fields
                      sign
                      time-values
                      fractional-value
                      use-fractional-value
                      precision
                      use-precision
                      ^long fractional-precision
                      use-fractional-precision]}]

           (let [[leading-unit ending-unit] fields

                 precision (if use-precision precision 2)

                 fractional-precision
                 (cond
                   (not= "SECOND" ending-unit) 0
                   use-fractional-precision fractional-precision
                   :else 2)

                 fractional-value
                 (if (pos? fractional-precision)
                   (mod fractional-value (parse-long (str/join (repeat fractional-precision "9"))))
                   0)

                 use-fractional-value (and (= "SECOND" ending-unit) use-fractional-value)

                 v (str sign
                        (loop [fields (drop-while #(not= leading-unit %) ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
                               s ""]
                          (if-some [field (first fields)]
                            (let [prefix (case field
                                           "MONTH" "-"
                                           "HOUR" " "
                                           ":")]
                              (if (= ending-unit field)
                                (str s prefix (time-values field))
                                (recur (rest fields) (if (= field leading-unit)
                                                       (mod (time-values leading-unit) (parse-long (str/join (repeat precision "9"))))
                                                       (str s prefix (time-values field))))))
                            (throw (Exception. "Unreachable"))))
                        (when use-fractional-value (str "." fractional-value)))]

             (list 'multi-field-interval v leading-unit precision ending-unit fractional-precision))))))

(def interval-constructor-gen
  (tcg/one-of [single-interval-constructor-gen multi-interval-constructor-gen]))

(tct/defspec all-possible-interval-literals-can-be-constructed-prop
  ;; gonna give this a few more rounds by default due to domain size
  1000
  (tcp/for-all [form interval-constructor-gen]
    (instance? PeriodDuration (project1 form {}))))

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
    [42] '(trim-array a 1) {:a [42, 43]}))

(t/deftest test-trim-array-offset-over-trim-exception
  (t/is (thrown-with-msg? IllegalArgumentException #"Data exception - array element error\." (project1 '(trim-array [1 2 3] 4) {}))))

(t/deftest test-cast-numerics
  (letfn [(test-cast
            ([src tgt-type] (test-cast src tgt-type {}))
            ([src tgt-type params] (project1 (list 'cast src tgt-type) params)))]
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

    (t/is (= 42 (test-cast 'a :i32 {:a 42.0})))))
