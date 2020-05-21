(ns crux.index-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.index :as idx]))

(t/deftest test-can-perform-unary-join
  (let [a-idx (idx/new-relation-virtual-index :a
                                              [[0]
                                               [1]
                                               [3]
                                               [4]
                                               [5]
                                               [6]
                                               [7]
                                               [8]
                                               [9]
                                               [11]
                                               [12]]
                                              1
                                              c/->value-buffer)
        b-idx (idx/new-relation-virtual-index :b
                                              [[0]
                                               [2]
                                               [6]
                                               [7]
                                               [8]
                                               [9]
                                               [12]]
                                              1
                                              c/->value-buffer)
        c-idx (idx/new-relation-virtual-index :c
                                              [[2]
                                               [4]
                                               [5]
                                               [8]
                                               [10]
                                               [12]]
                                              1
                                              c/->value-buffer)]

    (t/is (= [{:x 8}
              {:x 12}]
             (for [[_ join-results] (-> (idx/new-unary-join-virtual-index [(assoc a-idx :name :x)
                                                                           (assoc b-idx :name :x)
                                                                           (assoc c-idx :name :x)])
                                        (idx/idx->seq))]
               join-results)))))

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
  (let [r (idx/new-relation-virtual-index :r
                                          [[1 3]
                                           [1 4]
                                           [1 5]
                                           [3 5]]
                                          2
                                          c/->value-buffer)
        s (idx/new-relation-virtual-index :s
                                          [[3 4]
                                           [3 5]
                                           [4 6]
                                           [4 8]
                                           [4 9]
                                           [5 2]]
                                          2
                                          c/->value-buffer)
        t (idx/new-relation-virtual-index :t
                                          [[1 4]
                                           [1 5]
                                           [1 6]
                                           [1 8]
                                           [1 9]
                                           [1 2]
                                           [3 2]]
                                          2
                                          c/->value-buffer)]
    (t/testing "n-ary join"
      (let [index-groups [[(assoc r :name :a) (assoc t :name :a)]
                          [(assoc r :name :b) (assoc s :name :b)]
                          [(assoc s :name :c) (assoc t :name :c)]]
            result (-> (mapv idx/new-unary-join-virtual-index index-groups)
                       (idx/new-n-ary-join-layered-virtual-index)
                       (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                       (idx/layered-idx->seq))]
        (t/is (= [{:a 1, :b 3, :c 4}
                  {:a 1, :b 3, :c 5}
                  {:a 1, :b 4, :c 6}
                  {:a 1, :b 4, :c 8}
                  {:a 1, :b 4, :c 9}
                  {:a 1, :b 5, :c 2}
                  {:a 3, :b 5, :c 2}]
                 (for [[_ join-results] result]
                   join-results)))))))

(t/deftest test-sorted-virtual-index
  (let [idx (idx/new-sorted-virtual-index
             [[(c/->value-buffer 1) :a]
              [(c/->value-buffer 3) :c]])]
    (t/is (= :a
             (second (db/seek-values idx (c/->value-buffer 0)))))
    (t/is (= :a
             (second (db/seek-values idx (c/->value-buffer 1)))))
    (t/is (= :c
             (second (db/next-values idx))))
    (t/is (= :c
             (second (db/seek-values idx (c/->value-buffer 2)))))
    (t/is (= :c
             (second (db/seek-values idx (c/->value-buffer 3)))))
    (t/is (nil? (db/seek-values idx (c/->value-buffer 4))))))

(t/deftest test-range-predicates
  (let [r (idx/new-relation-virtual-index :r
                                          [[1]
                                           [2]
                                           [3]
                                           [4]
                                           [5]]
                                          1
                                          c/->value-buffer)]

    (t/is (= [1 2 3 4 5]
             (->> (idx/idx->seq r)
                  (map second))))

    (t/is (= [1 2 3 4 5]
             (into []
                   (map second)
                   (idx/idx->series r))))

    (t/is (= [1 2 3]
             (->> (idx/idx->seq (idx/new-less-than-virtual-index r (c/->value-buffer 4)))
                  (map second))))

    (t/is (= [1 2 3 4]
             (->> (idx/idx->seq (idx/new-less-than-equal-virtual-index r (c/->value-buffer 4)))
                  (map second))))

    (t/is (= [3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-virtual-index r (c/->value-buffer 2)))
                  (map second))))

    (t/is (= [2 3 4 5]
             (->> (idx/idx->seq (idx/new-greater-than-equal-virtual-index r (c/->value-buffer 2)))
                  (map second))))

    (t/is (= [2]
             (->> (idx/idx->seq (idx/new-equals-virtual-index r (c/->value-buffer 2)))
                  (map second))))

    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (c/->value-buffer 0)))))
    (t/is (empty? (idx/idx->seq (idx/new-equals-virtual-index r (c/->value-buffer 6)))))

    (t/testing "seek skips to lower range"
      (t/is (= 2 (second (db/seek-values (idx/new-greater-than-equal-virtual-index r (c/->value-buffer 2)) (c/->value-buffer nil)))))
      (t/is (= 3 (second (db/seek-values (idx/new-greater-than-virtual-index r (c/->value-buffer 2)) (c/->value-buffer 1))))))

    (t/testing "combining indexes"
      (t/is (= [2 3 4]
               (->> (idx/idx->seq (-> r
                                      (idx/new-greater-than-equal-virtual-index (c/->value-buffer 2))
                                      (idx/new-less-than-virtual-index (c/->value-buffer 5))))
                    (map second)))))

    (t/testing "incompatible type"
      (t/is (empty? (->> (idx/idx->seq (-> (idx/new-greater-than-equal-virtual-index r (c/->value-buffer "foo"))))
                         (map second)))))))

(t/deftest test-or-virtual-index
  (let [idx-1 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 1) :a]
                [(c/->value-buffer 3) :c]
                [(c/->value-buffer 5) :e1]])
        idx-2 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 2) :b]
                [(c/->value-buffer 4) :d]
                [(c/->value-buffer 5) :e2]
                [(c/->value-buffer 7) :g]])
        idx-3 (idx/new-sorted-virtual-index
               [[(c/->value-buffer 5) :e3]
                [(c/->value-buffer 6) :f]])
        idx (idx/new-or-virtual-index [idx-1 idx-2 idx-3])]
    (t/testing "interleaves results in value order"
      (t/is (= :a
               (second (db/seek-values idx nil))))
      (t/is (= :b
               (second (db/next-values idx))))
      (t/is (= :c
               (second (db/next-values idx))))
      (t/is (= :d
               (second (db/next-values idx)))))
    (t/testing "shared values are returned in index order"
      (t/is (= :e1
               (second (db/next-values idx))))
      (t/is (= :e2
               (second (db/next-values idx))))
      (t/is (= :e3
               (second (db/next-values idx)))))
    (t/testing "can continue after one index is done"
      (t/is (= :f
               (second (db/next-values idx))))
      (t/is (= :g
               (second (db/next-values idx)))))
    (t/testing "returns nil after all indexes are done"
      (t/is (nil? (db/next-values idx))))

    (t/testing "can seek into indexes"
      (t/is (= :d
               (second (db/seek-values idx (c/->value-buffer 4)))))
      (t/is (= :e1
               (second (db/next-values idx)))))))

;; NOTE: variable order must align up with relation position order
;; here. This implies that a relation cannot use the same variable
;; twice in two positions. All relations and the join order must be in
;; the same order for it to work.
(t/deftest test-n-ary-join-based-on-relational-tuples
  (let [r-idx (idx/new-relation-virtual-index :r
                                              [[7 4]
                                               ;; extra sanity check
                                               [8 4]]
                                              2
                                              c/->value-buffer)
        s-idx (idx/new-relation-virtual-index :s
                                              [[4 0]
                                               [4 1]
                                               [4 2]
                                               [4 3]]
                                              2
                                              c/->value-buffer)
        t-idx (idx/new-relation-virtual-index :t
                                              [[7 0]
                                               [7 1]
                                               [7 2]
                                               [8 1]
                                               [8 2]]
                                              2
                                              c/->value-buffer)
        index-groups [[(assoc r-idx :name :a)
                       (assoc t-idx :name :a)]
                      [(assoc r-idx :name :b)
                       (assoc s-idx :name :b)]
                      [(assoc s-idx :name :c)
                       (assoc t-idx :name :c)]]]
    (t/is (= #{[7 4 0]
               [7 4 1]
               [7 4 2]
               [8 4 1]
               [8 4 2]}
             (set (for [[_ join-results] (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                             (idx/new-n-ary-join-layered-virtual-index)
                                             (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                             (idx/layered-idx->seq))]
                    (vec (for [var [:a :b :c]]
                           (get join-results var)))))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-unary-conjunction-and-disjunction
  (let [p-idx (idx/new-relation-virtual-index :p
                                              [[1]
                                               [2]
                                               [3]]
                                              1
                                              c/->value-buffer)
        q-idx (idx/new-relation-virtual-index :q
                                              [[2]
                                               [3]
                                               [4]]
                                              1
                                              c/->value-buffer)
        r-idx (idx/new-relation-virtual-index :r
                                              [[3]
                                               [4]
                                               [5]]
                                              1
                                              c/->value-buffer)]
    (t/testing "conjunction"
      (let [unary-and-idx (idx/new-unary-join-virtual-index [(assoc p-idx :name :x)
                                                             (assoc q-idx :name :x)
                                                             (assoc r-idx :name :x)])]
        (t/is (= #{[3]}
                 (set (for [[_ join-results] (-> (idx/new-n-ary-join-layered-virtual-index [unary-and-idx])
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x]]
                               (get join-results var)))))))))

    (t/testing "disjunction"
      (let [unary-or-idx (idx/new-or-virtual-index
                          [(idx/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                           (idx/new-unary-join-virtual-index [(assoc q-idx :name :x)
                                                              (assoc r-idx :name :x)])])]
        (t/is (= #{[1]
                   [2]
                   [3]
                   [4]}
                 (set (for [[_ join-results] (-> (idx/new-n-ary-join-layered-virtual-index [unary-or-idx])
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x]]
                               (get join-results var)))))))))))

(t/deftest test-n-ary-join-based-on-relational-tuples-with-n-ary-conjunction-and-disjunction
  (let [p-idx (idx/new-relation-virtual-index :p
                                              [[1 3]
                                               [2 4]
                                               [2 20]]
                                              2
                                              c/->value-buffer)
        q-idx (idx/new-relation-virtual-index :q
                                              [[1 10]
                                               [2 20]
                                               [3 30]]
                                              2
                                              c/->value-buffer)
        index-groups [[(assoc p-idx :name :x)
                       (assoc q-idx :name :x)]
                      [(assoc p-idx :name :y)]
                      [(assoc q-idx :name :z)]]]
    (t/testing "conjunction"
      (t/is (= #{[1 3 10]
                 [2 4 20]
                 [2 20 20]}
               (set (for [[_ join-results] (-> (mapv idx/new-unary-join-virtual-index index-groups)
                                               (idx/new-n-ary-join-layered-virtual-index)
                                               (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                               (idx/layered-idx->seq))]
                      (vec (for [var [:x :y :z]]
                             (get join-results var))))))))

    (t/testing "disjunction"
      (let [zero-idx (idx/new-relation-virtual-index :zero
                                                     [[0]]
                                                     1
                                                     c/->value-buffer)
            lhs-index (idx/new-n-ary-join-layered-virtual-index
                       [(idx/new-unary-join-virtual-index [(assoc p-idx :name :x)])
                        (idx/new-unary-join-virtual-index [(assoc p-idx :name :y)])
                        (idx/new-unary-join-virtual-index [(assoc zero-idx :name :z)])])]
        (t/is (= #{[1 3 0]
                   [2 4 0]
                   [2 20 0]}
                 (set (for [[_ join-results] (-> lhs-index
                                                 (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                 (idx/layered-idx->seq))]
                        (vec (for [var [:x :y :z]]
                               (get join-results var)))))))
        (let [rhs-index (idx/new-n-ary-join-layered-virtual-index
                         [(idx/new-unary-join-virtual-index [(assoc q-idx :name :x)])
                          (idx/new-unary-join-virtual-index [(assoc zero-idx :name :y)])
                          (idx/new-unary-join-virtual-index [(assoc q-idx :name :z)])])]
          (t/is (= #{[1 0 10]
                     [2 0 20]
                     [3 0 30]}
                   (set (for [[_ join-results] (-> rhs-index
                                                   (idx/new-n-ary-constraining-layered-virtual-index idx/constrain-join-result-by-empty-names)
                                                   (idx/layered-idx->seq))]
                          (vec (for [var [:x :y :z]]
                                 (get join-results var))))))))))))
