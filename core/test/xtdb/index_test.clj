(ns xtdb.index-test
  (:require [clojure.test :as t]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.index :as idx])
  (:import clojure.lang.Box))

(defn new-relation-virtual-index
  ([rel] (new-relation-virtual-index rel (count (first rel))))

  ([rel max-depth]
   (idx/new-relation-virtual-index (mapv #(mapv c/->value-buffer %) rel) max-depth)))

(t/deftest test-can-perform-unary-join
  (let [a-idx (new-relation-virtual-index [[0] [1] [3] [4] [5] [6] [7] [8] [9] [11] [12]])
        b-idx (new-relation-virtual-index [[0] [2] [6] [7] [8] [9] [12]])
        c-idx (new-relation-virtual-index [[2] [4] [5] [8] [10] [12]])]

    (t/is (= [8 12]
             (->> (idx/new-unary-join-virtual-index [a-idx b-idx c-idx])
                  (idx/idx->seq)
                  (map c/decode-value-buffer))))))

;; Q(a, b, c) ← R(a, b), S(b, c), T (a, c).

;; (1, 3, 4)
;; (1, 3, 5)
;; (1, 4, 6)
;; (1, 4, 8)
;; (1, 4, 9)
;; (1, 5, 2)
;; (3, 5, 2)
;; TODO: Same as above.
(t/deftest test-can-perform-n-ary-join
  (let [r (new-relation-virtual-index [[1 3]
                                       [1 4]
                                       [1 5]
                                       [3 5]])
        s (new-relation-virtual-index [[3 4]
                                       [3 5]
                                       [4 6]
                                       [4 8]
                                       [4 9]
                                       [5 2]])
        t (new-relation-virtual-index [[1 4]
                                       [1 5]
                                       [1 6]
                                       [1 8]
                                       [1 9]
                                       [1 2]
                                       [3 2]])]
    (t/testing "n-ary join"
      (let [index-groups [[r t]
                          [r s]
                          [s t]]
            result (-> (mapv idx/new-unary-join-virtual-index index-groups)
                       (idx/new-n-ary-join-layered-virtual-index)
                       (idx/layered-idx->seq))]
        (t/is (= [[1 3 4]
                  [1 3 5]
                  [1 4 6]
                  [1 4 8]
                  [1 4 9]
                  [1 5 2]
                  [3 5 2]]
                 (for [join-keys result]
                   (mapv c/decode-value-buffer join-keys))))))))

(t/deftest test-range-predicates
  (let [r (new-relation-virtual-index [[1] [2] [3] [4] [5]])]

    (t/is (= [1 2 3 4 5]
             (->> (idx/idx->seq r)
                  (map c/decode-value-buffer))))

    (t/is (= [1 2 3]
             (->> (idx/idx->seq (idx/new-less-than-virtual-index r (Box. (c/->value-buffer 4))))
                  (map c/decode-value-buffer))))

    (t/is (= [1 2 3 4]
             (->> (idx/idx->seq (idx/new-less-than-equal-virtual-index r (Box. (c/->value-buffer 4))))
                  (map c/decode-value-buffer))))

    (t/is (= [3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (= [2 3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (= [2]
             (->> (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 2))))
                  (map c/decode-value-buffer))))

    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 0))))))
    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (Box. (c/->value-buffer 6))))))

    (t/testing "seek skips to lower range"
      (t/is (= 2 (c/decode-value-buffer (db/seek-values (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer 2))) (c/->value-buffer nil)))))
      (t/is (= 3 (c/decode-value-buffer (db/seek-values (idx/new-greater-than-virtual-index r (Box. (c/->value-buffer 2))) (c/->value-buffer 1))))))

    (t/testing "combining indexes"
      (t/is (= [2 3 4]
               (->> (idx/idx->seq (-> r
                                      (idx/new-greater-than-equal-virtual-index (Box. (c/->value-buffer 2)))
                                      (idx/new-less-than-virtual-index (Box. (c/->value-buffer 5)))))
                    (map c/decode-value-buffer)))))

    (t/testing "incompatible type"
      (t/is (empty? (->> (idx/idx->seq (-> (idx/new-greater-than-equal-virtual-index r (Box. (c/->value-buffer "foo")))))
                         (map c/decode-value-buffer)))))))

;; NOTE: variable order must align up with relation position order
;; here. This implies that a relation cannot use the same variable
;; twice in two positions. All relations and the join order must be in
;; the same order for it to work.
(t/deftest test-n-ary-join-based-on-relational-tuples
  (let [r-idx (new-relation-virtual-index [[7 4]
                                           ;; extra sanity check
                                           [8 4]])
        s-idx (new-relation-virtual-index [[4 0]
                                           [4 1]
                                           [4 2]
                                           [4 3]])
        t-idx (new-relation-virtual-index [[7 0]
                                           [7 1]
                                           [7 2]
                                           [8 1]
                                           [8 2]])
        index-groups [[r-idx t-idx]
                      [r-idx s-idx]
                      [s-idx t-idx]]]
    (t/is (= #{[7 4 0]
               [7 4 1]
               [7 4 2]
               [8 4 1]
               [8 4 2]}
             (set (for [join-keys (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                      (idx/new-n-ary-join-layered-virtual-index)
                                      (idx/layered-idx->seq))]
                    (mapv c/decode-value-buffer join-keys)))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-unary-conjunction
  (let [p-idx (new-relation-virtual-index [[1] [2] [3]])
        q-idx (new-relation-virtual-index [[2] [3] [4]])
        r-idx (new-relation-virtual-index [[3] [4] [5]])]
    (t/testing "conjunction"
      (let [unary-and-idx (idx/new-unary-join-virtual-index [p-idx
                                                             q-idx
                                                             r-idx])]
        (t/is (= #{[3]}
                 (set (for [join-keys (-> (idx/new-n-ary-join-layered-virtual-index [unary-and-idx])
                                          (idx/layered-idx->seq))]
                        (mapv c/decode-value-buffer join-keys)))))))))

(t/deftest test-empty-unary-join
  (let [p-idx (new-relation-virtual-index [] 1)
        q-idx (new-relation-virtual-index [] 1)]
    (t/is (empty? (idx/idx->seq (idx/new-unary-join-virtual-index [p-idx q-idx]))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-n-ary-conjunction-and-disjunction
  (let [p-idx (new-relation-virtual-index [[1 3]
                                           [2 4]
                                           [2 20]])
        q-idx (new-relation-virtual-index [[1 10]
                                           [2 20]
                                           [3 30]])
        index-groups [[p-idx q-idx]
                      [p-idx]
                      [q-idx]]]
    (t/testing "conjunction"
      (t/is (= #{[1 3 10]
                 [2 4 20]
                 [2 20 20]}
               (set (for [join-keys (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                        (idx/new-n-ary-join-layered-virtual-index)
                                        (idx/layered-idx->seq))]
                      (mapv c/decode-value-buffer join-keys))))))

    (t/testing "disjunction"
      (let [zero-idx (new-relation-virtual-index [[0]])
            lhs-index (idx/new-n-ary-join-layered-virtual-index
                       [(idx/new-unary-join-virtual-index [p-idx])
                        (idx/new-unary-join-virtual-index [p-idx])
                        (idx/new-unary-join-virtual-index [zero-idx])])]
        (t/is (= #{[1 3 0]
                   [2 4 0]
                   [2 20 0]}
                 (set (for [join-keys (-> lhs-index
                                          (idx/layered-idx->seq))]
                        (mapv c/decode-value-buffer join-keys)))))
        (let [rhs-index (idx/new-n-ary-join-layered-virtual-index
                         [(idx/new-unary-join-virtual-index [q-idx])
                          (idx/new-unary-join-virtual-index [zero-idx])
                          (idx/new-unary-join-virtual-index [q-idx])])]
          (t/is (= #{[1 0 10]
                     [2 0 20]
                     [3 0 30]}
                   (set (for [join-keys (-> rhs-index
                                            (idx/layered-idx->seq))]
                          (mapv c/decode-value-buffer join-keys))))))))))
