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
  (let [node tu/*node*]
    (insert-test-data node)
    (t/is (= [{:name "Alice"} {:name "Bob"} {:name "Charlie"} {:name "David"} {:name "Eve"}]
             (->> (xt/q node '(union
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name)))
          "union removes duplicates")))

(t/deftest test-union-all-operator
  (let [node tu/*node*]
    (insert-test-data node)
    (t/is (= 6
             (count (xt/q node '(union-all
                                 (from :table1 [{:name name}])
                                 (from :table2 [{:name name}])))))
          "union-all keeps duplicates - Alice appears twice")))

(t/deftest test-intersect-operator
  (let [node tu/*node*]
    (insert-test-data node)


    (t/is (= [{:name "Alice"}]
             (xt/q node '(intersect
                          (from :table1 [{:name name}])
                          (from :table2 [{:name name}]))))
          "intersect finds common elements without duplicates")))

(t/deftest test-intersect-all-operator
  (let [node tu/*node*]
    (insert-test-data node)


    (t/is (= [{:name "Alice"}]
             (xt/q node '(intersect-all
                          (from :table1 [{:name name}])
                          (from :table2 [{:name name}]))))
          "intersect-all finds common elements with duplicates")))

(t/deftest test-except-operator
  (let [node tu/*node*]
    (insert-test-data node)


    (t/is (= [{:name "Bob"} {:name "Charlie"}]
             (->> (xt/q node '(except
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name)))
          "except finds elements in first but not second without duplicates")))

(t/deftest test-except-all-operator
  (let [node tu/*node*]
    (insert-test-data node)


    (t/is (= [{:name "Bob"} {:name "Charlie"}]
             (->> (xt/q node '(except-all
                               (from :table1 [{:name name}])
                               (from :table2 [{:name name}])))
                  (sort-by :name)))
          "except-all finds elements in first but not second with duplicates"))

(t/deftest test-distinct-operator
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :table3 {:xt/id 1, :name "Alice"}]
                              [:put-docs :table3 {:xt/id 2, :name "Alice"}]
                        [:put-docs :table3 {:xt/id 3, :name "Bob"}]])
    (t/is (= 2
             (count (xt/q node '(distinct (from :table3 [{:name name}])))))
          "distinct removes duplicates"))))

(t/deftest test-set-operators-with-parameters
  (let [node tu/*node*]
    (insert-test-data node)

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
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :employees {:xt/id 1, :name "Alice", :dept "Engineering", :salary 85000}]
                              [:put-docs :employees {:xt/id 2, :name "Bob", :dept "Engineering", :salary 90000}]
                              [:put-docs :employees {:xt/id 3, :name "Charlie", :dept "Marketing", :salary 70000}]
                              [:put-docs :employees {:xt/id 4, :name "David", :dept "Engineering", :salary 95000}]
                              [:put-docs :contractors {:xt/id 5, :name "Eve", :dept "Engineering", :salary 80000}]
                        [:put-docs :contractors {:xt/id 6, :name "Frank", :dept "Marketing", :salary 65000}]])
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
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :customers {:xt/id 1, :name "Alice", :region "North"}]
                              [:put-docs :customers {:xt/id 2, :name "Bob", :region "South"}]
                              [:put-docs :customers {:xt/id 3, :name "Charlie", :region "North"}]
                              [:put-docs :orders {:xt/id 101, :customer-id 1, :amount 500}]
                              [:put-docs :orders {:xt/id 102, :customer-id 1, :amount 300}]
                        [:put-docs :orders {:xt/id 103, :customer-id 3, :amount 200}]])
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
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :sales {:xt/id 1, :product "A", :quarter "Q1", :amount 1000, :region "US"}]
                              [:put-docs :sales {:xt/id 2, :product "B", :quarter "Q1", :amount 1500, :region "US"}]
                              [:put-docs :sales {:xt/id 3, :product "A", :quarter "Q2", :amount 1200, :region "US"}]
                              [:put-docs :sales {:xt/id 4, :product "A", :quarter "Q1", :amount 800, :region "EU"}]
                        [:put-docs :sales {:xt/id 5, :product "B", :quarter "Q2", :amount 1800, :region "EU"}]])
    (t/is (= 5
             (count (xt/q node '(distinct
                                 (union-all
                                   (-> (from :sales [{:product prod, :quarter q, :amount amt, :region rgn}])
                                       (where (= rgn "US"))
                                       (return {:product prod, :period q, :revenue amt, :market "domestic"}))
                                   (-> (from :sales [{:product prod, :quarter q, :amount amt, :region rgn}])
                                       (where (= rgn "EU"))
                                       (return {:product prod, :period q, :revenue amt, :market "international"}))))))))

    (t/is (= #{}
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
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :table-a {:xt/id 1, :value "x"}]
                              [:put-docs :table-a {:xt/id 2, :value "y"}]
                              [:put-docs :table-b {:xt/id 3, :value "y"}]
                              [:put-docs :table-b {:xt/id 4, :value "z"}]
                              [:put-docs :table-c {:xt/id 5, :value "z"}]
                        [:put-docs :table-c {:xt/id 6, :value "w"}]])
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
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :numbers {:xt/id 1, :num 10, :category "even"}]
                              [:put-docs :numbers {:xt/id 2, :num 15, :category "odd"}]
                              [:put-docs :numbers {:xt/id 3, :num 20, :category "even"}]
                              [:put-docs :more-numbers {:xt/id 4, :num 15, :category "odd"}]
                              [:put-docs :more-numbers {:xt/id 5, :num 25, :category "odd"}]
                        [:put-docs :more-numbers {:xt/id 6, :num 30, :category "even"}]])
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
(comment
  (remove-ns 'xtdb.xtql-set-operators-test))

;; Comprehensive multiplicity (cardinality) tests for intersect and except operators
(t/deftest test-intersect-multiplicity-handling
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :left-set {:xt/id 1, :value "A"}]
                              [:put-docs :left-set {:xt/id 2, :value "A"}]
                              [:put-docs :left-set {:xt/id 3, :value "A"}]
                              [:put-docs :left-set {:xt/id 4, :value "B"}]
                              [:put-docs :left-set {:xt/id 5, :value "B"}]
                              [:put-docs :left-set {:xt/id 6, :value "C"}]
                              [:put-docs :right-set {:xt/id 7, :value "A"}]
                              [:put-docs :right-set {:xt/id 8, :value "A"}]
                              [:put-docs :right-set {:xt/id 9, :value "B"}]
                              [:put-docs :right-set {:xt/id 10, :value "B"}]
                        [:put-docs :right-set {:xt/id 11, :value "B"}]])
    (t/is (= {"A" 2, "B" 2}
             (-> (xt/q node '(intersect-all
                              (from :left-set [{:value value}])
                              (from :right-set [{:value value}])))
                 (->> (map :value))
                 (frequencies)))
          "intersect-all: cardinalities should be minimum of each side")

    ;; intersect: should remove duplicates (max 1 of each)
    (t/is (= #{"A" "B"}
             (->> (xt/q node '(intersect
                               (from :left-set [{:value value}])
                               (from :right-set [{:value value}])))
                  (map :value)
                  (set)))
          "intersect: should return unique values only")))

(t/deftest test-except-multiplicity-handling
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :left-set {:xt/id 1, :value "A"}]
                              [:put-docs :left-set {:xt/id 2, :value "A"}]
                              [:put-docs :left-set {:xt/id 3, :value "A"}]
                              [:put-docs :left-set {:xt/id 4, :value "B"}]
                              [:put-docs :left-set {:xt/id 5, :value "B"}]
                              [:put-docs :left-set {:xt/id 6, :value "C"}]
                              [:put-docs :right-set {:xt/id 7, :value "A"}]
                              [:put-docs :right-set {:xt/id 8, :value "A"}]
                        [:put-docs :right-set {:xt/id 9, :value "B"}]])
    (t/is (= {"A" 1, "B" 1, "C" 1}
             (-> (xt/q node '(except-all
                              (from :left-set [{:value value}])
                              (from :right-set [{:value value}])))
                 (->> (map :value))
                 (frequencies)))
          "except-all: cardinalities should be difference of left minus right")

    (t/is (= #{"A" "B" "C"}
             (->> (xt/q node '(except
                               (from :left-set [{:value value}])
                               (from :right-set [{:value value}])))
                  (map :value)
                  (set)))
          "except: should return unique values from the except-all result")))

(t/deftest test-except-vs-except-all-semantics
  (let [node tu/*node*]
    (xt/submit-tx node [;; Left has: X, Y, Y, Z
                              [:put-docs :left-only-set {:xt/id 1, :value "X"}]
                              [:put-docs :left-only-set {:xt/id 2, :value "Y"}]
                              [:put-docs :left-only-set {:xt/id 3, :value "Y"}]
                              [:put-docs :left-only-set {:xt/id 4, :value "Z"}]
                        ;; Right has: Y (so Y should be excluded from result)
                        [:put-docs :right-filter-set {:xt/id 5, :value "Y"}]])
    (t/is (= {"X" 1, "Y" 1, "Z" 1}
             (-> (xt/q node '(except-all
                              (from :left-only-set [{:value value}])
                              (from :right-filter-set [{:value value}])))
                 (->> (map :value))
                 (frequencies)))
          "except-all: should subtract cardinalities")

    ;; except: should return unique {X, Y, Z} since except-all returns all three
    (t/is (= #{"X" "Y" "Z"}
             (->> (xt/q node '(except
                               (from :left-only-set [{:value value}])
                               (from :right-filter-set [{:value value}])))
                  (map :value)
                  (set)))
          "except: should return unique values from except-all result")))

(t/deftest test-edge-cases-multiplicity
  (let [node tu/*node*]

    ;; Empty sets
    (t/testing "intersect with empty sets"
      (xt/submit-tx node [[:put-docs :empty-test {:xt/id 1, :value "A"}]])
      (t/is (empty? (xt/q node '(intersect-all
                                   (from :empty-test [{:value value}])
                                   (from :non-existent [{:value value}])))))

        (t/is (empty? (xt/q node '(intersect
                                   (from :empty-test [{:value value}])
                                   (from :non-existent [{:value value}])))))

    ;; Single element sets
    (t/testing "intersect with single elements"
      (xt/submit-tx node [[:put-docs :single-a {:xt/id 1, :value "X"}]
                          [:put-docs :single-b {:xt/id 2, :value "X"}]])
      (t/is (= [{"X" 1}]
                 [(-> (xt/q node '(intersect-all
                                   (from :single-a [{:value value}])
                                   (from :single-b [{:value value}])))
                      (->> (map :value))
                      (frequencies))]))

        (t/is (= #{"X"}
                 (->> (xt/q node '(intersect
                                   (from :single-a [{:value value}])
                                   (from :single-b [{:value value}])))
                      (map :value)
                      (set))))

    ;; Except with empty right side
    (t/testing "except with empty right side"
      (xt/submit-tx node [[:put-docs :left-only {:xt/id 1, :value "Y"}]
                          [:put-docs :left-only {:xt/id 2, :value "Y"}]])
      (t/is (= {"Y" 2}
                 (-> (xt/q node '(except-all
                                  (from :left-only [{:value value}])
                                  (from :non-existent [{:value value}])))
                     (->> (map :value))
                     (frequencies)))
              "except-all with empty right side should return all left elements")

        (t/is (= #{"Y"}
                 (->> (xt/q node '(except
                                   (from :left-only [{:value value}])
                                   (from :non-existent [{:value value}])))
                      (map :value)
                      (set)))
              "except with empty right side should return unique left elements")))))

;; Test set operators as tail operators
(t/deftest test-set-operators-as-tail-operators
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :employees {:xt/id 1, :name "Alice", :dept "Engineering"}]
                              [:put-docs :employees {:xt/id 2, :name "Bob", :dept "Marketing"}]
                              [:put-docs :employees {:xt/id 3, :name "Charlie", :dept "Engineering"}]
                              [:put-docs :contractors {:xt/id 4, :name "David", :dept "Engineering"}]
                        [:put-docs :contractors {:xt/id 5, :name "Eve", :dept "Marketing"}]])
    (t/is (= #{"Alice" "Bob" "Charlie" "David" "Eve"}
             (->> (xt/q node '(-> (from :employees [{:name name}])
                                  (union (from :contractors [{:name name}]))))
                  (map :name)
                  (set)))
          "union as tail operator should combine current pipeline with another query")

    ;; Union-all as tail operator
    (t/is (= 5  ; All 5 people, no deduplication needed in this case
             (count (xt/q node '(-> (from :employees [{:name name}])
                                    (union-all (from :contractors [{:name name}]))))))
          "union-all as tail operator should preserve duplicates")

    ;; Intersect as tail operator: current pipeline INTERSECT another query
    (t/is (= #{"Engineering" "Marketing"}  ; Both depts exist in both employees and contractors
             (->> (xt/q node '(-> (from :employees [{:dept dept}])
                                  (intersect (from :contractors [{:dept dept}]))))
                  (map :dept)
                  (set)))
          "intersect as tail operator should find common elements")

    ;; Except as tail operator: current pipeline EXCEPT another query
    (t/is (= #{"Bob"}  ; Bob is the only marketing employee not also a contractor name
             (->> (xt/q node '(-> (from :employees [{:name name, :dept dept}])
                                  (where (= dept "Marketing"))
                                  (return {:name name})
                                  (except (from :contractors [{:name name}]))))
                  (map :name)
                  (set)))
          "except as tail operator should find elements only in current pipeline")

    ;; Distinct as tail operator
    (t/is (= #{"Engineering" "Marketing"}
             (->> (xt/q node '(-> (from :employees [{:dept dept}])
                                  (union-all (from :contractors [{:dept dept}]))
                                  (distinct)))
                  (map :dept)
                  (set)))
          "distinct as tail operator should remove duplicates from pipeline")))

;; Additional comprehensive multiplicity test scenarios
(t/deftest test-complex-multiplicity-scenarios
  (let [node tu/*node*]
    (xt/submit-tx node [;; Dataset with varied multiplicities for comprehensive testing
                              [:put-docs :multi-left {:xt/id 1, :key "A", :category "X"}]
                              [:put-docs :multi-left {:xt/id 2, :key "A", :category "X"}] ; A appears 2x
                              [:put-docs :multi-left {:xt/id 3, :key "B", :category "Y"}] ; B appears 1x
                              [:put-docs :multi-left {:xt/id 4, :key "C", :category "Z"}]
                              [:put-docs :multi-left {:xt/id 5, :key "C", :category "Z"}]
                              [:put-docs :multi-left {:xt/id 6, :key "C", :category "Z"}] ; C appears 3x
                              [:put-docs :multi-right {:xt/id 7, :key "A", :category "X"}]
                              [:put-docs :multi-right {:xt/id 8, :key "A", :category "X"}]
                              [:put-docs :multi-right {:xt/id 9, :key "A", :category "X"}] ; A appears 3x
                              [:put-docs :multi-right {:xt/id 10, :key "B", :category "Y"}]
                              [:put-docs :multi-right {:xt/id 11, :key "B", :category "Y"}] ; B appears 2x
                        [:put-docs :multi-right {:xt/id 12, :key "D", :category "W"}] ; D appears 1x (only in right)
                        ])

    (t/testing "intersect-all with varied multiplicities"
      ;; Left: A(2), B(1), C(3)
      ;; Right: A(3), B(2), D(1)
      ;; intersect-all result: A(min(2,3)=2), B(min(1,2)=1), C(min(3,0)=0), D(min(0,1)=0)
      (t/is (= {"A" 2, "B" 1}
               (-> (xt/q node '(intersect-all
                                (from :multi-left [{:key key}])
                                (from :multi-right [{:key key}])))
                   (->> (map :key))
                   (frequencies)))
            "intersect-all: should return minimum multiplicities"))

    (t/testing "except-all with varied multiplicities"
      ;; Left: A(2), B(1), C(3)
      ;; Right: A(3), B(2), D(1)
      ;; except-all result: A(max(0,2-3)=0), B(max(0,1-2)=0), C(max(0,3-0)=3), D(max(0,0-1)=0)
      (t/is (= {"C" 3}
               (-> (xt/q node '(except-all
                                (from :multi-left [{:key key}])
                                (from :multi-right [{:key key}])))
                   (->> (map :key))
                   (frequencies)))
            "except-all: should return left minus right multiplicities"))

    (t/testing "union-all preserves all multiplicities"
      ;; Left: A(2), B(1), C(3)
      ;; Right: A(3), B(2), D(1)
      ;; union-all result: A(2+3=5), B(1+2=3), C(3+0=3), D(0+1=1)
      (t/is (= {"A" 5, "B" 3, "C" 3, "D" 1}
               (-> (xt/q node '(union-all
                                (from :multi-left [{:key key}])
                                (from :multi-right [{:key key}])))
                   (->> (map :key))
                   (frequencies)))
            "union-all: should sum multiplicities from both sides"))

    (t/testing "distinct operations remove all duplicates"
      ;; Each distinct operation should return max 1 of each element
      (t/is (= #{"A" "B" "C"}
               (->> (xt/q node '(distinct (from :multi-left [{:key key}])))
                    (map :key)
                    (set)))
            "distinct: should return unique elements only")

      (t/is (= #{"A" "B" "D"}
               (->> (xt/q node '(distinct (from :multi-right [{:key key}])))
                    (map :key)
                    (set)))
            "distinct: should return unique elements only"))))

(t/deftest test-multiplicity-with-tail-operators
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :base-data {:xt/id 1, :value "P"}]
                              [:put-docs :base-data {:xt/id 2, :value "P"}] ; P appears 2x
                              [:put-docs :base-data {:xt/id 3, :value "Q"}] ; Q appears 1x
                              [:put-docs :tail-data {:xt/id 4, :value "P"}] ; P appears 1x
                        [:put-docs :tail-data {:xt/id 5, :value "R"}] ; R appears 1x
                        ])

    (t/testing "union-all as tail operator preserves multiplicities"
      ;; Base pipeline: P(2), Q(1)
      ;; Tail query: P(1), R(1)
      ;; Result: P(2+1=3), Q(1+0=1), R(0+1=1)
      (t/is (= {"P" 3, "Q" 1, "R" 1}
               (-> (xt/q node '(-> (from :base-data [{:value value}])
                                   (union-all (from :tail-data [{:value value}]))))
                   (->> (map :value))
                   (frequencies)))
            "union-all as tail: should sum multiplicities"))

    (t/testing "intersect-all as tail operator uses minimum multiplicities"
      ;; Base pipeline: P(2), Q(1)
      ;; Tail query: P(1), R(1)
      ;; Result: P(min(2,1)=1), Q(min(1,0)=0), R(min(0,1)=0)
      (t/is (= {"P" 1}
               (-> (xt/q node '(-> (from :base-data [{:value value}])
                                   (intersect-all (from :tail-data [{:value value}]))))
                   (->> (map :value))
                   (frequencies)))
            "intersect-all as tail: should use minimum multiplicities"))

    (t/testing "except-all as tail operator subtracts multiplicities"
      ;; Base pipeline: P(2), Q(1)
      ;; Tail query: P(1), R(1)
      ;; Result: P(max(0,2-1)=1), Q(max(0,1-0)=1), R(max(0,0-1)=0)
      (t/is (= {"P" 1, "Q" 1}
               (-> (xt/q node '(-> (from :base-data [{:value value}])
                                   (except-all (from :tail-data [{:value value}]))))
                   (->> (map :value))
                   (frequencies)))
            "except-all as tail: should subtract multiplicities"))))

(t/deftest test-multiplicity-through-pipelines
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :pipeline-test {:xt/id 1, :item "Alpha", :count 5}]
                              [:put-docs :pipeline-test {:xt/id 2, :item "Alpha", :count 3}]
                              [:put-docs :pipeline-test {:xt/id 3, :item "Beta", :count 7}]
                              [:put-docs :pipeline-test {:xt/id 4, :item "Beta", :count 7}] ; Duplicate Beta-7 pair
                              [:put-docs :other-data {:xt/id 5, :item "Alpha", :count 5}]  ; Exact match for first Alpha
                        [:put-docs :other-data {:xt/id 6, :item "Beta", :count 2}]   ; Different count for Beta
                        ])

    (t/testing "multiplicity preserved through complex pipeline before set operation"
      ;; This tests that multiplicities are correctly handled when set operations
      ;; are applied to the results of complex transformations
      (t/is (= {"Alpha" 3, "Beta" 3} ; Left: Alpha(2), Beta(2) + Right: Alpha(1), Beta(1) = Alpha(3), Beta(3)
               (-> (xt/q node '(-> (from :pipeline-test [{:item item, :count cnt}])
                                   (where (>= cnt 3))  ; Filters keep: Alpha(5,3), Beta(7,7)
                                   (return {:item item}) ; Projects to just item names
                                   (union-all (-> (from :other-data [{:item item, :count cnt}])
                                                  (where (< cnt 6))   ; Filters keep: Alpha(5), Beta(2)
                                                  (return {:item item})))))
                   (->> (map :item))
                   (frequencies)))
            "complex pipeline before union-all should preserve correct multiplicities")

      ;; Test with intersect-all after pipeline transformations
      (t/is (= {"Alpha" 1}  ; Only Alpha-5 matches between the filtered results
               (-> (xt/q node '(-> (from :pipeline-test [{:item item, :count cnt}])
                                   (intersect-all (from :other-data [{:item item, :count cnt}]))))
                   (->> (map :item))
                   (frequencies)))
            "intersect-all should find minimum multiplicities of exact matches"))))

;; Tests for variadic vs binary operator behavior
(t/deftest test-variadic-set-operations
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :set-a {:xt/id 1, :value "x"}]
                              [:put-docs :set-a {:xt/id 2, :value "y"}]
                              [:put-docs :set-b {:xt/id 3, :value "y"}]
                              [:put-docs :set-b {:xt/id 4, :value "z"}]
                              [:put-docs :set-c {:xt/id 5, :value "z"}]
                        [:put-docs :set-c {:xt/id 6, :value "w"}]])

    (t/testing "3-way union (variadic)"
      (t/is (= #{"x" "y" "z" "w"}
               (->> (xt/q node '(union
                                 (from :set-a [{:value value}])
                                 (from :set-b [{:value value}])
                                 (from :set-c [{:value value}])))
                    (map :value)
                    (set)))
            "3-way union should work"))

    (t/testing "3-way union-all (variadic)"
      ;; Should preserve all occurrences: y appears in A and B (2 total), z appears in B and C (2 total)
      (t/is (= {"x" 1, "y" 2, "z" 2, "w" 1}
               (->> (xt/q node '(union-all
                                 (from :set-a [{:value value}])
                                 (from :set-b [{:value value}])
                                 (from :set-c [{:value value}])))
                    (map :value)
                    (frequencies)))
            "3-way union-all should preserve all occurrences"))

    (t/testing "3-way intersect (variadic)"
      ;; A = {x, y}, B = {y, z}, C = {z, w}
      ;; A ∩ B ∩ C = {} (no element appears in all three)
      (t/is (= #{}
               (->> (xt/q node '(intersect
                                 (from :set-a [{:value value}])
                                 (from :set-b [{:value value}])
                                 (from :set-c [{:value value}])))
                    (map :value)
                    (set)))
            "3-way intersect should find common elements (none in this case)"))

    (t/testing "3-way intersect with actual common element"
      (xt/submit-tx node [[:put-docs :set-a {:xt/id 10, :value "common"}]
                          [:put-docs :set-b {:xt/id 11, :value "common"}]
                          [:put-docs :set-c {:xt/id 12, :value "common"}]])
      (t/is (= #{"common"}
                 (->> (xt/q node '(intersect
                                   (from :set-a [{:value value}])
                                   (from :set-b [{:value value}])
                                   (from :set-c [{:value value}])))
                      (map :value)
                      (set)))
              "3-way intersect should find the common element"))

    (t/testing "3-way intersect-all (variadic)"
      (xt/submit-tx node [[:put-docs :set-a {:xt/id 13, :value "multi"}]   ; multi appears 1x in A
                                  [:put-docs :set-b {:xt/id 14, :value "multi"}]   ; multi appears 1x in B
                                  [:put-docs :set-b {:xt/id 15, :value "multi"}]   ; multi appears 2x total in B
                                  [:put-docs :set-c {:xt/id 16, :value "multi"}]   ; multi appears 1x in C
                                  [:put-docs :set-c {:xt/id 17, :value "multi"}]   ; multi appears 2x total in C
                          [:put-docs :set-c {:xt/id 18, :value "multi"}]]) ; multi appears 3x total in C
      (t/is (= {"multi" 1}
                 (->> (xt/q node '(intersect-all
                                   (from :set-a [{:value value}])
                                   (from :set-b [{:value value}])
                                   (from :set-c [{:value value}])))
                      (map :value)
                      (frequencies)
                      (filter #(= (key %) "multi"))
                      (into {})))
              "3-way intersect-all should use minimum multiplicity")))))

(t/deftest test-binary-except-operations
  (let [node tu/*node*]
    (xt/submit-tx node [[:put-docs :left-set {:xt/id 1, :value "a"}]
                              [:put-docs :left-set {:xt/id 2, :value "b"}]
                              [:put-docs :left-set {:xt/id 3, :value "c"}]
                              [:put-docs :right-set {:xt/id 4, :value "b"}]
                        [:put-docs :right-set {:xt/id 5, :value "d"}]])

    (t/testing "except is strictly binary"
      (t/is (= #{"a" "c"}
               (->> (xt/q node '(except
                                 (from :left-set [{:value value}])
                                 (from :right-set [{:value value}])))
                    (map :value)
                    (set)))
            "except should work with exactly 2 arguments"))

    (t/testing "except-all is strictly binary"
      ;; Same result for this data since each element appears once
      (t/is (= #{"a" "c"}
               (->> (xt/q node '(except-all
                                 (from :left-set [{:value value}])
                                 (from :right-set [{:value value}])))
                    (map :value)
                    (set)))
            "except-all should work with exactly 2 arguments"))))

(t/deftest test-operator-arity-validation
  (let [node tu/*node*]

    (t/testing "union requires at least 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(union (rel [{:x "a"}] [x])))))
      (t/is (= #{"a" "b"}
               (->> (xt/q node '(union
                                 (rel [{:x "a"}] [x])
                                 (rel [{:x "b"}] [x])))
                    (map :x)
                    (set))))
      (t/is (= #{"a" "b" "c"}
               (->> (xt/q node '(union
                                 (rel [{:x "a"}] [x])
                                 (rel [{:x "b"}] [x])
                                 (rel [{:x "c"}] [x])))
                    (map :x)
                    (set)))))

    (t/testing "union-all requires at least 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(union-all (rel [{:x "a"}] [x]))))))

    (t/testing "intersect requires at least 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(intersect (rel [{:x "a"}] [x]))))))

    (t/testing "intersect-all requires at least 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(intersect-all (rel [{:x "a"}] [x]))))))

    (t/testing "except requires exactly 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(except (rel [{:x "a"}] [x])))))
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(except
                                  (rel [{:x "a"}] [x])
                                  (rel [{:x "b"}] [x])
                                  (rel [{:x "c"}] [x]))))))

    (t/testing "except-all requires exactly 2 queries"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(except-all (rel [{:x "a"}] [x])))))
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(except-all
                                  (rel [{:x "a"}] [x])
                                  (rel [{:x "b"}] [x])
                                  (rel [{:x "c"}] [x]))))))

    (t/testing "distinct requires exactly 1 query"
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(distinct))))
      (t/is (thrown? Exception
                     #_:clj-kondo/ignore
                     (xt/q node '(distinct
                                  (rel [{:x "a"}] [x])
                                  (rel [{:x "b"}] [x]))))))))
