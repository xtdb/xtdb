(ns xtdb.scalar-predicate-test
  (:require [clojure.test :as t]
            [xtdb.expression :as expr]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr])
  (:import (java.time LocalDate)
           (xtdb.arrow RelationReader VectorIndirection$Mask VectorIndirection$Mask$Builder VectorReader)
           (xtdb.expression ScalarPredicate ScalarPredicate$Bound ScalarPredicateSelection)
           (xtdb.operator SelectionSpec$Factory)))

(t/use-fixtures :each tu/with-allocator)

(def ^:private nan Double/NaN)

(defn- open-rel ^RelationReader [cols]
  (vr/keyed-rel-reader (into {} (map (fn [[nm type vals]] [(str nm) (tu/open-vec type (vec vals))])) cols)
                       (count (nth (first cols) 2))))

(defn- types-of [^RelationReader rel]
  (into {} (map (fn [[col-name ^VectorReader col]] [(symbol col-name) (.getType col)])) (.getCols rel)))

(defn- input-types [rel args]
  {:var-types (types-of rel), :param-types (types-of args)})

(defn- select-rows [^SelectionSpec$Factory factory ^RelationReader rel args]
  (with-open [spec (.open factory tu/*allocator* {} args)]
    (let [^VectorIndirection$Mask m (.select spec rel)]
      [(.getRowCount m) (vec (.toSelection m))])))

(defn- kernel-factory ^SelectionSpec$Factory [form input-types]
  (expr/->expression-selection-spec (expr/form->expr form input-types) input-types))

(defn- generated-factory ^SelectionSpec$Factory [form input-types]
  (#'expr/->generated-selection-spec (expr/form->expr form input-types) input-types))

(defn- bind ^ScalarPredicate$Bound [form rel args]
  (let [input-types (input-types rel args)]
    (some-> ^ScalarPredicate (#'expr/->scalar-predicate (expr/form->expr form input-types))
            (.bind args))))

(defn- kernel-answers?
  [form rel args]
  (some? (some-> (bind form rel args) (.select rel (VectorIndirection$Mask/all (.getRowCount ^RelationReader rel))))))

(defn- check-form [form rel args]
  (let [input-types (input-types rel args)
        kernel (kernel-factory form input-types)]
    (t/is (instance? ScalarPredicateSelection kernel) (pr-str form))
    (t/is (= (select-rows (generated-factory form input-types) rel args)
             (select-rows kernel rel args))
          (pr-str form))))

(defn- mask-of ^VectorIndirection$Mask [n pred]
  (let [b (VectorIndirection$Mask$Builder. n)]
    (doseq [i (range n) :when (pred i)] (.add b i))
    (.build b)))

(defn- views [^RelationReader rel]
  (let [n (.getRowCount rel)]
    {"rel" rel
     "selection" (.select rel (int-array (filter even? (range n))))
     "reversed selection" (.select rel (int-array (reverse (range n))))
     "selection of each row twice, out of order" (.select rel (int-array (concat (reverse (range n)) (range n))))
     "masked" (.select rel (mask-of n #(not= 1 (mod % 3))))}))

(def ^:private int-vals [0 1 -1 5 -5 127 -128 nil 3 nil 5 4 6 2 100 nil nil 5])
(def ^:private flt-vals [0.0 -0.0 1.5 nan -1.5 5.0 nil Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY 2.5 nan nil 5.0 -0.0 3.25 0.0 nil nan])
(def ^:private date-vals (concat [nil] (for [d [0 1 18000 18262 18263 -5 19000]] (LocalDate/ofEpochDay d)) (repeat 10 nil)))

(def ^:private col-specs
  [['i8 (types/->type '[:? :i8]) (map #(some-> % byte) int-vals)]
   ['i16 (types/->type '[:? :i16]) (map #(some-> % (* 100) short) int-vals)]
   ['i32 (types/->type '[:? :i32]) (map #(some-> % (* 100000) int) int-vals)]
   ['i64 (types/->type '[:? :i64]) (concat (map #(some-> % (* 3000000000)) (butlast int-vals)) [9007199254740993])]
   ['f32 (types/->type '[:? :f32]) (map #(some-> % unchecked-float) flt-vals)]
   ['f64 (types/->type '[:? :f64]) flt-vals]
   ['d (types/->type '[:? :date :day]) date-vals]])

(def ^:private scalars
  [0 5 -3 500 3000000000 15000000000 9007199254740992 2.5 -0.0 0.0 nan 5.0 9.007199254740992E15 0.1 (LocalDate/ofEpochDay 18262)])

(defn- scalar-domain [scalar]
  (condp instance? scalar Long :long, Double :double, LocalDate :date))

(def ^:private covered-pairs
  #{['i8 :long] ['i8 :double] ['i16 :long] ['i16 :double] ['i32 :long] ['i32 :double] ['i64 :long] ['i64 :double]
    ['f32 :long] ['f32 :double] ['f64 :long] ['f64 :double] ['d :date]})

(t/deftest comparisons-equal-generated-code
  (with-open [base (open-rel col-specs)]
    (let [!answered (atom #{})]
      (doseq [[view-name rel] (views base)
              [col _ _] col-specs
              scalar scalars
              :when (contains? covered-pairs [col (scalar-domain scalar)])
              op '[< <= > >= == <>]]
        (t/testing (str view-name " " col " " op " " (pr-str scalar))
          (with-open [args (tu/open-args {:p scalar})]
            (doseq [form [(list op col scalar)
                          (list op scalar col)
                          (list op col '?p)
                          (list op '?p col)]]
              (check-form form rel args)
              (when (kernel-answers? form rel args)
                (swap! !answered conj [col (scalar-domain scalar)]))))))

      (t/is (= covered-pairs @!answered) "every covered pair is answered by the kernel, not the fallback"))))

(t/deftest non-matching-shapes-keep-generated-code
  (let [input-types {:var-types {'a (types/->type :i64), 'b (types/->type :i64), 's (types/->type :utf8)}, :param-types {}}]
    (doseq [form '[(< a b) (< (+ a 1) 3) (== s :x) (< s "x") (>= "x" s) (and (< a 1) (< a b)) (nil? a) (< a 1.5M)
                   (< a nil) (not (< a 1)) (and (< a 1) (> a 0)) (or (< a 1) (> a 2)) (== s "x")]]
      (t/is (not (instance? ScalarPredicateSelection (kernel-factory form input-types))) (pr-str form)))))

(t/deftest recognised-expressions-the-ee-cannot-compile-throw-at-plan-time
  (t/is (thrown? Throwable (kernel-factory '(< unknown 1) {:var-types {}, :param-types {}}))))

(t/deftest uncovered-batches-fall-back-to-generated-code
  (with-open [planned (open-rel [['x (types/->type '[:? :i64]) [1 nil 3]]])
              args (tu/open-args {:p 2})]
    (let [factory (kernel-factory '(< x ?p) (input-types planned args))]
      (doseq [[desc cols] [["decimal, which the vectors don't compare" [['x (types/->type '[:? :decimal 32 2 128]) [1.00M nil 3.00M 0.00M]]]]
                           ["a union of i64 and f64" [['x (types/->type '#{:i64 :f64}) [1 2.5 3 1.5]]]]
                           ["i64, the planned type" [['x (types/->type '[:? :i64]) [1 nil 3 0]]]]]]
        (with-open [rel (open-rel cols)]
          (t/is (= [4 [0 3]] (select-rows factory rel args)) desc)
          (t/is (= (select-rows (generated-factory '(< x ?p) (input-types rel args)) rel args)
                   (select-rows factory rel args))
                desc))))))

(t/deftest uncovered-params-take-generated-code-for-the-query
  (with-open [rel (open-rel [['x (types/->type '[:? :i64]) [1 nil 3 0]]])]
    (doseq [p [2.5M nil]]
      (with-open [args (tu/open-args {:p p})]
        (let [form '(< x ?p)]
          (t/is (not (kernel-answers? form rel args)) (pr-str p))
          (check-form form rel args))))))

(defn- select-under [^SelectionSpec$Factory factory ^RelationReader rel args ^VectorIndirection$Mask mask]
  (with-open [spec (.open factory tu/*allocator* {} args)]
    (let [^VectorIndirection$Mask m (.select spec rel mask)]
      [(.getRowCount m) (vec (.toSelection m))])))

(defn- input-masks [n]
  {"none" (mask-of n (constantly false))
   "all" (VectorIndirection$Mask/all n)
   "every third" (mask-of n #(zero? (mod % 3)))
   "all but the first and last" (mask-of n #(< 0 % (dec n)))})

(t/deftest an-input-mask-keeps-only-the-rows-it-sets
  (with-open [base (open-rel col-specs)
              args (tu/open-args {:p 5 :q 2.5})]
    (doseq [[view-name ^RelationReader rel] (views base)
            form '[(> i32 0)
                   (< f64 ?q)
                   (< i64 nil)
                   (nil? f64)]
            :let [input-types (input-types rel args)]
            [factory-name factory] {"kernel" (kernel-factory form input-types)
                                    "generated" (generated-factory form input-types)}
            [mask-name ^VectorIndirection$Mask mask] (input-masks (.getRowCount rel))]
      (t/is (= (let [[n rows] (select-rows factory rel args)]
                 [n (filterv #(.isSet mask %) rows)])
               (select-under factory rel args mask))
            (str view-name ", " factory-name " " (pr-str form) ", " mask-name)))))

(t/deftest an-input-mask-carries-through-the-per-batch-fallback
  (with-open [planned (open-rel [['x (types/->type '[:? :i64]) [1 nil 3]]])
              rel (open-rel [['x (types/->type '[:? :decimal 32 2 128]) [1.00M nil 3.00M 0.00M 1.00M]]])
              args (tu/open-args {:p 2})]
    (let [factory (kernel-factory '(< x ?p) (input-types planned args))]
      (t/is (= [5 [3 4]] (select-under factory rel args (mask-of 5 #(not= 0 %)))))
      (t/is (= [5 []] (select-under factory rel args (mask-of 5 (constantly false))))))))
