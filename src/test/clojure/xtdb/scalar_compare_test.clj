(ns xtdb.scalar-compare-test
  (:require [clojure.test :as t]
            [xtdb.expression :as expr]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr])
  (:import (java.time LocalDate)
           (xtdb.api.error Fault)
           (xtdb.arrow CmpOp RelationReader Vector VectorIndirection$Mask VectorReader)
           (xtdb.operator SelectionSpec$Factory)))

(t/use-fixtures :each tu/with-allocator)

(def ^:private nan Double/NaN)

(defn- open-rel ^RelationReader [cols]
  (vr/keyed-rel-reader (into {} (map (fn [[nm type vals]] [(str nm) (tu/open-vec type (vec vals))])) cols)
                       (count (nth (first cols) 2))))

(defn- var-types [^RelationReader rel]
  (into {} (map (fn [[col-name ^VectorReader col]] [(symbol col-name) (.getType col)])) (.getCols rel)))

(defn- mask-rows [^VectorIndirection$Mask m]
  [(.getRowCount m) (vec (.toSelection m))])

(defn- generated-select [form ^RelationReader rel ^RelationReader args]
  (let [input-types {:var-types (var-types rel), :param-types (var-types args)}
        ^SelectionSpec$Factory factory (#'expr/->generated-selection-spec (expr/form->expr form input-types) input-types)]
    (with-open [spec (.open factory tu/*allocator* {} args)]
      (mask-rows (.select spec rel)))))

(def ^:private cmp-ops {'< CmpOp/LT, '<= CmpOp/LTE, '> CmpOp/GT, '>= CmpOp/GTE, '== CmpOp/EQ, '<> CmpOp/NEQ})

(defn- compare-scalar ^VectorIndirection$Mask [^Vector v op scalar ^ints sel ^VectorIndirection$Mask m]
  (let [^CmpOp op (cmp-ops op)]
    (cond
      (instance? LocalDate scalar) (.compare v op ^LocalDate scalar sel m)
      (instance? Double scalar) (.compare v op (double scalar) sel m)
      :else (.compare v op (long scalar) sel m))))

(defn- vector-select [^Vector v ^ints sel row-count op scalar negated?]
  (let [all (VectorIndirection$Mask/all row-count)
        [n cmp-rows] (mask-rows (compare-scalar v op scalar sel all))]
    (if negated?
      (let [[_ not-null-rows] (mask-rows (.notNull v sel all))]
        [n (vec (remove (set cmp-rows) not-null-rows))])
      [n cmp-rows])))

(def ^:private int-vals [0 1 -1 5 -5 127 -128 nil 3 nil 5 4 6 2 100 nil nil 5])
(def ^:private flt-vals [0.0 -0.0 1.5 nan -1.5 5.0 nil Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY 2.5 nan nil 5.0 -0.0 3.25 0.0 nil nan])
(def ^:private date-vals (concat [nil] (for [d [0 1 18000 18262 18263 -5 19000]] (LocalDate/ofEpochDay d)) (repeat 10 nil)))

(def ^:private col-specs
  [['i8 (types/->type '[:? :i8]) (map #(some-> % byte) int-vals)]
   ['i16 (types/->type '[:? :i16]) (map #(some-> % (* 100) short) int-vals)]
   ['i32 (types/->type '[:? :i32]) (map #(some-> % (* 100000) int) int-vals)]
   ['i64 (types/->type '[:? :i64]) (map #(some-> % (* 3000000000)) int-vals)]
   ['f32 (types/->type '[:? :f32]) (map #(some-> % unchecked-float) flt-vals)]
   ['f64 (types/->type '[:? :f64]) flt-vals]
   ['d (types/->type '[:? :date :day]) (take (count int-vals) date-vals)]])

(def ^:private scalars [0 5 -3 500 3000000000 15000000000 2.5 -0.0 0.0 nan 5.0 0.1 (LocalDate/ofEpochDay 18262)])

(defn- covered? [col scalar]
  (if (= col 'd) (instance? LocalDate scalar) (not (instance? LocalDate scalar))))

(defn- cases [^RelationReader base]
  (let [n (.getRowCount base)]
    (for [[label idxs] [["vector" nil]
                        ["sel: even rows" (filter even? (range n))]
                        ["sel: reversed" (reverse (range n))]
                        ["sel: each row twice, out of order" (concat (reverse (range n)) (range n))]]
          :let [^ints sel (some-> idxs int-array)]]
      {:label label, :rel (if sel (.select base sel) base), :sel sel})))

(t/deftest scalar-comparisons-equal-generated-code
  (with-open [^RelationReader base (open-rel col-specs)
              args (tu/open-args {})]
    (let [checked (volatile! 0)]
      (doseq [{:keys [label ^RelationReader rel sel]} (cases base)
              [col _ _] col-specs
              op '[< <= > >= == <>]
              scalar scalars
              :when (covered? col scalar)
              negated? [false true]
              :let [form (if negated? (list 'not (list op col scalar)) (list op col scalar))]]
        (vswap! checked inc)
        (t/is (= (generated-select form rel args)
                 (vector-select (.vectorFor base (name col)) sel (.getRowCount rel) op scalar negated?))
              (pr-str label form)))
      (t/is (pos? @checked)))))

(t/deftest whole-validity-words-agree-with-gathered-validity
  (let [n 150]
    (with-open [^RelationReader rel (open-rel [['x (types/->type '[:? :i64]) (for [i (range n)] (when-not (= 3 (mod i 7)) (* 10 i)))]])]
      (let [v (.vectorFor rel "x")
            identity-sel (int-array (range n))]
        (doseq [op '[< >= <>]
                negated? [false true]]
          (t/is (= (vector-select v identity-sel n op 640 negated?)
                   (vector-select v nil n op 640 negated?))
                (pr-str op negated?)))))))

(t/deftest uncovered-pairs-answer-nil
  (with-open [^RelationReader base (open-rel (conj col-specs ['dec (types/->type '[:? :decimal 32 2 128]) (repeat (count int-vals) 1.25M)]))]
    (let [all (VectorIndirection$Mask/all (.getRowCount base))]
      (t/is (nil? (compare-scalar (.vectorFor base "d") '< 5 nil all)))
      (t/is (nil? (compare-scalar (.vectorFor base "i64") '< (LocalDate/ofEpochDay 1) nil all)))
      (t/is (nil? (compare-scalar (.vectorFor base "dec") '< 5 nil all)))
      (t/is (nil? (compare-scalar (.vectorFor base "dec") '< 2.5 nil all))))))

(t/deftest comparisons-refuse-a-mask-longer-than-the-vector
  (with-open [^RelationReader base (open-rel col-specs)]
    (let [n (.getRowCount base)]
      (doseq [[col _ _] col-specs
              :let [scalar (if (= col 'd) (LocalDate/ofEpochDay 0) 0)]
              [label ^Vector v ^VectorIndirection$Mask m sel] [["vector" (.vectorFor base (name col)) (VectorIndirection$Mask/all (inc n)) nil]
                                                                ["selection" (.vectorFor base (name col)) (VectorIndirection$Mask/all 3) (int-array [0 1])]]]
        (t/is (thrown? Fault (compare-scalar v '>= scalar sel m)) (str col " compare, " label))
        (t/is (thrown? Fault (.notNull v sel m)) (str col " notNull, " label))))))

(t/deftest byte-comparisons-refuse-a-mask-longer-than-the-vector
  (with-open [^RelationReader base (open-rel [['s (types/->type :utf8) ["a" "" "bc" "" "d" "e" "f"]]
                                        ['b (types/->type :varbinary) (map #(.getBytes ^String %) ["a" "" "bc" "" "d" "e" "f"])]])]
    (let [n (.getRowCount base)
          empty-bytes (byte-array 0)]
      (doseq [col ["s" "b"]
              [label ^Vector v ^VectorIndirection$Mask m ^ints sel] [["vector" (.vectorFor base col) (VectorIndirection$Mask/all (inc n)) nil]
                                                                     ["selection" (.vectorFor base col) (VectorIndirection$Mask/all 3) (int-array [0 1])]]]
        (t/is (thrown? Fault (.compare v CmpOp/EQ empty-bytes sel m)) (str col " eq, " label))
        (t/is (thrown? Fault (.notNull v sel m)) (str col " notNull, " label))))))
