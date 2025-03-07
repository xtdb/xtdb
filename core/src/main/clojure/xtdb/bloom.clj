(ns xtdb.bloom
  (:require [xtdb.expression :as expr]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (org.apache.arrow.memory RootAllocator)
           [org.apache.arrow.vector ValueVector]
           (xtdb.arrow RelationReader VectorReader)
           (xtdb.bloom BloomUtils)))

(set! *unchecked-math* :warn-on-boxed)

;; max-rows  bloom-bits  false-positive
;; 1000      13           0.0288
;; 10000     16           0.0495
;; 10000     17           0.0085
;; 100000    20           0.0154

(comment
  (defn bloom-false-positive-probability?
    (^double [^long n]
     (bloom-false-positive-probability? n BloomUtils/BLOOM_K BloomUtils/BLOOM_BITS))
    (^double [^long n ^long k ^long m]
     (Math/pow (- 1 (Math/exp (/ (- k) (double (/ m n))))) k))))

(def literal-hasher
  (-> (fn [value-expr target-col-type]
        (let [{:keys [return-type continue] :as emitted-expr}
              (expr/codegen-expr {:op :call
                                  :f :cast
                                  :args [value-expr]
                                  :target-type target-col-type}
                                 {:param-types (when (= :param (:op value-expr))
                                                 (let [{:keys [param param-type]} value-expr]
                                                   {param param-type}))})
              {:keys [writer-bindings write-value-out!]} (expr/write-value-out-code return-type)]
          (-> `(fn [~(-> expr/args-sym (expr/with-tag RelationReader))
                    ~(-> expr/out-vec-sym (expr/with-tag ValueVector))]
                 (let [~@(expr/batch-bindings emitted-expr)
                       ~@writer-bindings]
                   ~(continue (fn [return-type code]
                                `(do
                                   ~(write-value-out! return-type code)
                                   (BloomUtils/bloomHashes (VectorReader/from (vr/vec->reader ~expr/out-vec-sym)) 0))))))
              #_(doto (clojure.pprint/pprint))
              (eval))))
      (util/lru-memoize)))

(defn literal-hashes ^ints [params value-expr target-col-type]
  (let [f (literal-hasher value-expr target-col-type)]
    (with-open [allocator (RootAllocator.)
                tmp-vec (-> (types/col-type->field target-col-type)
                            (.createVector allocator))]
      (f params tmp-vec))))
