(ns xtdb.xtql-set-operators-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import (java.util Date)))

(t/use-fixtures :each tu/with-node)

(defn- insert-test-data [node]
  (xt/submit-tx node [[:put-docs :table1 {:xt/id 1, :name "Alice", :age 25}]
                      [:put-docs :table1 {:xt/id 2, :name "Bob", :age 30}]
                      [:put-docs :table1 {:xt/id 3, :name "Charlie", :age 35}]
                      [:put-docs :table2 {:xt/id 1, :name "Alice", :age 25}]
                      [:put-docs :table2 {:xt/id 4, :name "David", :age 30}]
                      [:put-docs :table2 {:xt/id 5, :name "Eve", :age 35}]]))

(t/deftest test-union-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that union removes duplicates
    (t/is (= [{:name "Alice"} {:name "Bob"} {:name "Charlie"} {:name "David"} {:name "Eve"}]
             (->> (xt/q node '(union
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name))))))

(t/deftest test-union-all-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that union-all keeps duplicates
    (t/is (= 6 ;; Alice appears twice: once from each table
             (count (xt/q node '(union-all
                                 (from :table1 [{:name name}])
                                 (from :table2 [{:name name}]))))))))

(t/deftest test-intersect-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that intersect finds common elements (without duplicates)
    (t/is (= [{:name "Alice"}]
             (xt/q node '(intersect
                          (from :table1 [{:name name}])
                          (from :table2 [{:name name}])))))))

(t/deftest test-intersect-all-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that intersect-all finds common elements (with duplicates)
    (t/is (= [{:name "Alice"}]
             (xt/q node '(intersect-all
                          (from :table1 [{:name name}])
                          (from :table2 [{:name name}])))))))

(t/deftest test-except-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that except finds elements in first but not second (without duplicates)
    (t/is (= [{:name "Bob"} {:name "Charlie"}]
             (->> (xt/q node '(except
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name))))))

(t/deftest test-except-all-operator
  (let [node tu/*node*
        _ (insert-test-data node)]

    ;; Test that except-all finds elements in first but not second (with duplicates)
    (t/is (= [{:name "Bob"} {:name "Charlie"}]
             (->> (xt/q node '(except-all
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name))))))

(t/deftest test-distinct-operator
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :table3 {:xt/id 1, :name "Alice"}]
                              [:put-docs :table3 {:xt/id 2, :name "Alice"}]
                              [:put-docs :table3 {:xt/id 3, :name "Bob"}]])]

    ;; Test that distinct removes duplicates
    (t/is (= 2 ;; Should only return unique names
             (count (xt/q node '(distinct (from :table3 [{:name name}]))))))))

;; Test parameter passing (implicit scoping like pipeline operator)
(t/deftest test-set-operators-with-parameters
  (let [node tu/*node*
        _ (insert-test-data node)]

    (t/is (= [{:result "Alice"}]
             (xt/q node ['(fn [$target-name]
                            (union
                              (-> (from :table1 [{:name name}])
                                  (where (= name $target-name))
                                  (return {:result name}))
                              (-> (from :table2 [{:name name}])
                                  (where (= name $target-name))
                                  (return {:result name}))))
                         "Alice"])))))

;; Complex integration tests with other XTQL features
(t/deftest test-set-operators-with-aggregation
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :employees {:xt/id 1, :name "Alice", :dept "Engineering", :salary 85000}]
                              [:put-docs :employees {:xt/id 2, :name "Bob", :dept "Engineering", :salary 90000}]
                              [:put-docs :employees {:xt/id 3, :name "Charlie", :dept "Marketing", :salary 70000}]
                              [:put-docs :employees {:xt/id 4, :name "David", :dept "Engineering", :salary 95000}]
                              [:put-docs :contractors {:xt/id 5, :name "Eve", :dept "Engineering", :salary 80000}]
                              [:put-docs :contractors {:xt/id 6, :name "Frank", :dept "Marketing", :salary 65000}]])]

    ;; Union with aggregation and grouping
    (t/is (= #{{:dept "Engineering", :total-people 4, :avg-salary 87500.0}
               {:dept "Marketing", :total-people 2, :avg-salary 67500.0}}
             (set (xt/q node '(-> (union-all
                                   (from :employees [{:dept dept, :salary salary}])
                                   (from :contractors [{:dept dept, :salary salary}]))
                                  (aggregate dept {:total-people (count dept), :avg-salary (avg salary)}))))))

    ;; Intersect with filtering - ensure both sides have same columns
    (t/is (= #{{:name "Alice"} {:name "Bob"} {:name "David"}}
             (set (xt/q node '(intersect
                               (-> (from :employees [{:name name, :dept dept}])
                                   (where (= dept "Engineering"))
                                   (return {:name name}))
                               (-> (from :employees [{:name name, :salary salary}])
                                   (where (> salary 80000))
                                   (return {:name name})))))))))

(t/deftest test-set-operators-with-subqueries-and-exists
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :customers {:xt/id 1, :name "Alice", :region "North"}]
                              [:put-docs :customers {:xt/id 2, :name "Bob", :region "South"}]
                              [:put-docs :customers {:xt/id 3, :name "Charlie", :region "North"}]
                              [:put-docs :orders {:xt/id 101, :customer-id 1, :amount 500}]
                              [:put-docs :orders {:xt/id 102, :customer-id 1, :amount 300}]
                              [:put-docs :orders {:xt/id 103, :customer-id 3, :amount 200}]])]

    ;; Union with exists subqueries - using parameterized subqueries
    (t/is (= #{{:name "Alice", :has-large-orders true}
               {:name "Charlie", :has-large-orders false}}
             (set (xt/q node '(distinct
                               (-> (from :customers [{:xt/id cust-id, :name name, :region region}])
                                   (where (= region "North"))
                                   (with {:has-large-orders (exists? [(fn [cust-id]
                                                                        (-> (from :orders [{:customer-id cid, :amount amt}])
                                                                            (where (and (= cid cust-id)
                                                                                        (> amt 400)))))
                                                                       cust-id])})
                                   (return {:name name, :has-large-orders has-large-orders})))))))

    ;; Intersect with explicit unify instead of problematic joins
    ;; Should find customers who both have orders AND are in North region
    ;; Alice (id: 1) - North region, has orders 101, 102 ✓
    ;; Charlie (id: 3) - North region, has order 103 ✓
    ;; Bob (id: 2) - South region, no orders ✗
    (t/is (= #{{:name "Alice"} {:name "Charlie"}}
             (set (xt/q node '(intersect
                               (-> (unify (from :customers [{:name name, :xt/id cust-id}])
                                          (from :orders [{:customer-id cid}])
                                          (where (= cid cust-id)))
                                   (return {:name name}))
                               (-> (from :customers [{:name name, :region region}])
                                   (where (= region "North"))
                                   (return {:name name})))))))))

(t/deftest test-set-operators-with-complex-expressions
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :sales {:xt/id 1, :product "A", :quarter "Q1", :amount 1000, :region "US"}]
                              [:put-docs :sales {:xt/id 2, :product "B", :quarter "Q1", :amount 1500, :region "US"}]
                              [:put-docs :sales {:xt/id 3, :product "A", :quarter "Q2", :amount 1200, :region "US"}]
                              [:put-docs :sales {:xt/id 4, :product "A", :quarter "Q1", :amount 800, :region "EU"}]
                              [:put-docs :sales {:xt/id 5, :product "B", :quarter "Q2", :amount 1800, :region "EU"}]])]

    ;; Union with complex expressions and calculated fields
    (t/is (= 4  ; US Q1+Q2 + EU Q1+Q2 unique combinations
             (count (xt/q node '(distinct
                                 (union-all
                                   (-> (from :sales [{:product prod, :quarter q, :amount amt, :region rgn}])
                                       (where (= rgn "US"))
                                       (return {:product prod, :period q, :revenue amt, :market "domestic"}))
                                   (-> (from :sales [{:product prod, :quarter q, :amount amt, :region rgn}])
                                       (where (= rgn "EU"))
                                       (return {:product prod, :period q, :revenue amt, :market "international"}))))))))

    ;; Except with mathematical operations
    (t/is (= #{{:product "B", :high-performer true}}
             (set (xt/q node '(-> (except
                                   (-> (from :sales [{:product prod, :amount amt}])
                                       (aggregate prod {:total-sales (sum amt)})
                                       (where (> total-sales 1500))
                                       (return {:product prod, :high-performer true}))
                                   (-> (from :sales [{:product prod, :region rgn}])
                                       (where (= rgn "EU"))
                                       (return {:product prod, :high-performer true})))
                                  (distinct))))))))

(t/deftest test-nested-set-operators
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :table-a {:xt/id 1, :value "x"}]
                              [:put-docs :table-a {:xt/id 2, :value "y"}]
                              [:put-docs :table-b {:xt/id 3, :value "y"}]
                              [:put-docs :table-b {:xt/id 4, :value "z"}]
                              [:put-docs :table-c {:xt/id 5, :value "z"}]
                              [:put-docs :table-c {:xt/id 6, :value "w"}]])]

    ;; Nested set operations: (A ∪ B) ∩ (B ∪ C)
    (t/is (= #{{:value "y"} {:value "z"}}
             (set (xt/q node '(intersect
                               (union
                                 (from :table-a [{:value value}])
                                 (from :table-b [{:value value}]))
                               (union
                                 (from :table-b [{:value value}])
                                 (from :table-c [{:value value}])))))))

    ;; Complex nesting: DISTINCT((A ∪ B) - (B ∩ C))
    ;; A = {x, y}, B = {y, z}, C = {z, w}
    ;; A ∪ B = {x, y, z}, B ∩ C = {z}
    ;; (A ∪ B) - (B ∩ C) = {x, y, z} - {z} = {x, y}
    (t/is (= #{{:value "x"} {:value "y"}}
             (set (xt/q node '(distinct
                               (except
                                 (union-all
                                   (from :table-a [{:value value}])
                                   (from :table-b [{:value value}]))
                                 (intersect
                                   (from :table-b [{:value value}])
                                   (from :table-c [{:value value}]))))))))))

(t/deftest test-set-operators-with-order-and-limit
  (let [node tu/*node*
        _ (xt/submit-tx node [[:put-docs :numbers {:xt/id 1, :num 10, :category "even"}]
                              [:put-docs :numbers {:xt/id 2, :num 15, :category "odd"}]
                              [:put-docs :numbers {:xt/id 3, :num 20, :category "even"}]
                              [:put-docs :more-numbers {:xt/id 4, :num 15, :category "odd"}]
                              [:put-docs :more-numbers {:xt/id 5, :num 25, :category "odd"}]
                              [:put-docs :more-numbers {:xt/id 6, :num 30, :category "even"}]])]

    ;; Union with ordering and limiting
    (t/is (= [{:num 10} {:num 15}]
             (xt/q node '(-> (union
                              (from :numbers [{:num num}])
                              (from :more-numbers [{:num num}]))
                             (order-by num)
                             (limit 2)))))

    ;; Distinct with complex ordering
    (t/is (= [{:num 30, :category "even"}
              {:num 25, :category "odd"}
              {:num 20, :category "even"}]
             (xt/q node '(-> (distinct
                              (union-all
                                (from :numbers [{:num num, :category category}])
                                (from :more-numbers [{:num num, :category category}])))
                             (order-by {:val num, :dir :desc})
                             (limit 3)))))))
