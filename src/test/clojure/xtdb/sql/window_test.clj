(ns xtdb.sql.window-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest test-window-functions
  (t/is (=plan-file
         "test-window-with-partition-and-order-by"
         (sql/plan "SELECT y, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) FROM docs"
                   {:table-info {#xt/table docs #{"y" "z"}}})))

  (t/is (thrown-with-msg? UnsupportedOperationException #"TODO"
                          (sql/plan "SELECT ROW_NUMBER() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM docs"
                                    {:table-info {#xt/table docs #{"y" "z"}}}))
        "window frames not yet supported")

  (t/testing "multiple window functions with different partition specs"
    (t/is (some? (sql/plan "SELECT
                              ROW_NUMBER() OVER (PARTITION BY y) AS rn,
                              ROW_NUMBER() OVER (PARTITION BY z) AS rn2
                            FROM docs"
                           {:table-info {#xt/table docs #{"y" "z"}}}))))

  ;; TODO similar to #3640
  #_
  (t/is (= nil
           (sql/plan "SELECT ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) FROM docs"
                     {:table-info {#xt/table docs #{"_id"}}})))

  (let [docs [{:a 1 :b 20}
              {:a 1 :b 10}
              {:a 2 :b 30}
              {:a 2 :b 40}
              {:a 1 :b 50}
              {:a 1 :b 60}
              {:a 2 :b 70}
              {:a 3 :b 80}
              {:a 3 :b 90}]]

    (xt/submit-tx tu/*node* [(into [:put-docs :docs ]
                                   (for [[id doc] (zipmap (range) docs)] (assoc doc :xt/id id)))])

    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 60, :rn 4}
               {:a 2, :b 30, :rn 1}
               {:a 2, :b 40, :rn 2}
               {:a 2, :b 70, :rn 3}
               {:a 3, :b 80, :rn 1}
               {:a 3, :b 90, :rn 2}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM docs")
                  set)))

    (t/is (= #{{:a 1, :b 60, :rn 1}
               {:a 1, :b 10, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 20, :rn 4}
               {:a 2, :b 70, :rn 1}
               {:a 2, :b 30, :rn 2}
               {:a 2, :b 40, :rn 3}
               {:a 3, :b 90, :rn 1}
               {:a 3, :b 80, :rn 2}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a) AS rn FROM docs")
                  set))
          "only partition by")

    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 2, :b 30, :rn 3}
               {:a 2, :b 40, :rn 4}
               {:a 1, :b 50, :rn 5}
               {:a 1, :b 60, :rn 6}
               {:a 2, :b 70, :rn 7}
               {:a 3, :b 80, :rn 8}
               {:a 3, :b 90, :rn 9}}
             (->> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (ORDER BY b) AS rn FROM docs")
                  set))
          "only order by")

    (t/is (= #{{:a 1, :b 60, :rn 1}
               {:a 2, :b 70, :rn 2}
               {:a 2, :b 30, :rn 3}
               {:a 3, :b 90, :rn 4}
               {:a 1, :b 10, :rn 5}
               {:a 3, :b 80, :rn 6}
               {:a 1, :b 50, :rn 7}
               {:a 2, :b 40, :rn 8}
               {:a 1, :b 20, :rn 9}}
             (-> (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER () AS rn FROM docs")
                 set))
          "nothing")
    #_
    (t/is (= [{:a 1, :b 60, :rn 1}
              {:a 2, :b 70, :rn 2}
              {:a 2, :b 30, :rn 3}
              {:a 3, :b 90, :rn 4}
              {:a 1, :b 10, :rn 5}
              {:a 3, :b 80, :rn 6}
              {:a 1, :b 50, :rn 7}
              {:a 2, :b 40, :rn 8}
              {:a 1, :b 20, :rn 9}]
             (xt/q tu/*node* "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY y ORDER BY z) AS rn FROM docs"))
          "no existing columns")))

(t/deftest test-lead-lag-window-functions
  (xt/submit-tx tu/*node* [[:put-docs :docs
                            {:xt/id 0, :a 1, :b 10}
                            {:xt/id 1, :a 1, :b 20}
                            {:xt/id 2, :a 1, :b 30}
                            {:xt/id 3, :a 2, :b 40}
                            {:xt/id 4, :a 2, :b 50}]])

  (t/is (= #{{:a 1, :b 10}
             {:a 1, :b 20, :prevb 10}
             {:a 1, :b 30, :prevb 20}
             {:a 2, :b 40}
             {:a 2, :b 50, :prevb 40}}
           (->> (xt/q tu/*node* "SELECT a, b, LAG(b, 1) OVER (PARTITION BY a ORDER BY b) AS prevb FROM docs")
                set))
        "LAG returns previous value within partition")

  (t/is (= #{{:a 1, :b 10, :nextb 20}
             {:a 1, :b 20, :nextb 30}
             {:a 1, :b 30}
             {:a 2, :b 40, :nextb 50}
             {:a 2, :b 50}}
           (->> (xt/q tu/*node* "SELECT a, b, LEAD(b, 1) OVER (PARTITION BY a ORDER BY b) AS nextb FROM docs")
                set))
        "LEAD returns next value within partition")

  (t/is (= #{{:a 1, :b 10, :nextb 30}
             {:a 1, :b 20}
             {:a 1, :b 30}
             {:a 2, :b 40}
             {:a 2, :b 50}}
           (->> (xt/q tu/*node* "SELECT a, b, LEAD(b, 2) OVER (PARTITION BY a ORDER BY b) AS nextb FROM docs")
                set))
        "LEAD with offset 2")

  (t/is (= #{{:a 1, :b 10}
             {:a 1, :b 20, :prevb 10}
             {:a 1, :b 30, :prevb 20}
             {:a 2, :b 40}
             {:a 2, :b 50, :prevb 40}}
           (->> (xt/q tu/*node* "SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS prevb FROM docs")
                set))
        "LAG with default offset of 1"))

(t/deftest test-multiple-window-functions-5211
  (xt/submit-tx tu/*node* [[:put-docs :docs
                            {:xt/id 0, :x 1, :y "A", :z 100}
                            {:xt/id 1, :x 1, :y "A", :z 200}
                            {:xt/id 2, :x 1, :y "B", :z 100}
                            {:xt/id 3, :x 2, :y "A", :z 100}
                            {:xt/id 4, :x 2, :y "B", :z 200}]])

  (t/testing "two window functions with different partition specs"
    ;; x=1 has ids 0,1,2 → rn-x = 1,2,3
    ;; x=2 has ids 3,4 → rn-x = 1,2
    ;; y="A" has ids 0,1,3 → rn-y = 1,2,3
    ;; y="B" has ids 2,4 → rn-y = 1,2
    ;; Note: without ORDER BY, the numbering within partitions is arbitrary
    ;; We just check that each partition has sequential numbers 1..n
    (let [results (->> (xt/q tu/*node* "SELECT x, y,
                                               ROW_NUMBER() OVER (PARTITION BY x) AS rn_x,
                                               ROW_NUMBER() OVER (PARTITION BY y) AS rn_y
                                        FROM docs")
                       set)]
      (t/is (= 5 (count results)))
      ;; Check rn-x numbering: x=1 should have 1,2,3 and x=2 should have 1,2
      (t/is (= #{1 2 3} (set (map :rn-x (filter #(= 1 (:x %)) results)))))
      (t/is (= #{1 2} (set (map :rn-x (filter #(= 2 (:x %)) results)))))
      ;; Check rn-y numbering: y="A" should have 1,2,3 and y="B" should have 1,2
      (t/is (= #{1 2 3} (set (map :rn-y (filter #(= "A" (:y %)) results)))))
      (t/is (= #{1 2} (set (map :rn-y (filter #(= "B" (:y %)) results)))))))

  (t/testing "multiple windows with same spec share numbering correctly"
    (let [results (->> (xt/q tu/*node* "SELECT x,
                                               ROW_NUMBER() OVER (PARTITION BY x) AS rn1,
                                               ROW_NUMBER() OVER (PARTITION BY x) AS rn2
                                        FROM docs")
                       set)]
      (t/is (= 5 (count results)))
      ;; Both rn1 and rn2 should have the same values for each row
      (t/is (every? #(= (:rn1 %) (:rn2 %)) results))))

  (t/testing "mixed: different functions with different partition specs"
    (let [results (->> (xt/q tu/*node* "SELECT x, y, z,
                                               ROW_NUMBER() OVER (PARTITION BY x) AS rn,
                                               LAG(z, 1) OVER (PARTITION BY y ORDER BY z) AS prev_z
                                        FROM docs")
                       set)]
      (t/is (= 5 (count results)))
      ;; Check ROW_NUMBER by x partition
      (t/is (= #{1 2 3} (set (map :rn (filter #(= 1 (:x %)) results)))))
      (t/is (= #{1 2} (set (map :rn (filter #(= 2 (:x %)) results)))))
      ;; Check LAG by y partition, ordered by z
      ;; y="A": z values are 100, 100, 200 (from ids 0, 3, 1)
      ;;   sorted by z: 100 (id 0 or 3), 100 (id 0 or 3), 200 (id 1)
      ;;   prev_z: nil, 100, 100
      ;; y="B": z values are 100, 200 (from ids 2, 4)
      ;;   sorted by z: 100 (id 2), 200 (id 4)
      ;;   prev_z: nil, 100
      (let [y-a-results (filter #(= "A" (:y %)) results)
            y-b-results (filter #(= "B" (:y %)) results)]
        ;; For y="A", there should be one nil and two with prev-z=100
        (t/is (= 1 (count (filter #(nil? (:prev-z %)) y-a-results))))
        ;; For y="B", first row has nil prev-z, second has 100
        (t/is (= 1 (count (filter #(nil? (:prev-z %)) y-b-results))))
        (t/is (= 1 (count (filter #(= 100 (:prev-z %)) y-b-results))))))))
