(ns xtdb.update-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest update-doesnt-put-unchanged-versions-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :a 1}
                             {:xt/id 2, :a 2}
                             {:xt/id 3, :a 3, :b 3}
                             {:xt/id 4, :a 4, :b 4}
                             {:xt/id 5, :a 5, :b nil}
                             {:xt/id 6, :a nil}
                             {:xt/id 7, :a 7}]])
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 1 WHERE _id = 1"])
  (t/is (= [{:xt/id 1, :a 1, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 1"))
        "update all cols, but unchanged")

  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 2, b = 2 WHERE _id = 2"])
  (t/is (= [{:xt/id 2, :a 2, :xt/valid-from #xt/zdt "2020-01-01[UTC]", :xt/valid-to #xt/zdt "2020-01-03[UTC]"}
            {:xt/id 2, :a 2, :b 2, :xt/valid-from #xt/zdt "2020-01-03[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 2 ORDER BY _valid_from"))
        "existing cols unchanged, add non-existing cols")

  (xt/execute-tx tu/*node* ["UPDATE docs SET b = 3 WHERE _id = 3"])
  (t/is (= [{:xt/id 3, :a 3, :b 3, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 3"))
        "col unchanged, other existing cols")

  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 4, b = 5 WHERE _id = 4"])
  (t/is (= [{:xt/id 4, :a 4, :b 4, :xt/valid-from #xt/zdt "2020-01-01[UTC]", :xt/valid-to #xt/zdt "2020-01-05[UTC]"}
            {:xt/id 4, :a 4, :b 5, :xt/valid-from #xt/zdt "2020-01-05[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 4 ORDER BY _valid_from"))
        "one col unchanged, one col changed")

  (t/testing "null handling"
    (xt/execute-tx tu/*node* ["UPDATE docs SET b = NULL WHERE _id = 5"])
    (t/is (= [{:xt/id 5, :a 5, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}]
             (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 5"))
          "update b from NULL to NULL - should not create new version")

    (xt/execute-tx tu/*node* ["UPDATE docs SET a = NULL WHERE _id = 6"])
    (t/is (= [{:xt/id 6, :xt/valid-from #xt/zdt "2020-01-01[UTC]"}]
             (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 6"))
          "update a from NULL to NULL - should not create new version")

    (xt/execute-tx tu/*node* ["UPDATE docs SET a = NULL WHERE _id = 7"])
    (t/is (= [{:xt/id 7, :a 7, :xt/valid-from #xt/zdt "2020-01-01[UTC]", :xt/valid-to #xt/zdt "2020-01-08[UTC]"}
              {:xt/id 7, :xt/valid-from #xt/zdt "2020-01-08[UTC]"}]
             (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME WHERE _id = 7 ORDER BY _valid_from"))
          "update a from value to NULL - should create new version")))

(t/deftest update-changes-types-if-requested-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a 1} {:xt/id 2, :a 2}]])
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 1.0 WHERE _id = 1"
                            "UPDATE docs SET a = 2 WHERE _id = 2"])

  (t/is (= [{:xt/id 1, :a 1,          
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 1, :a 1.0, :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 2, :a 2, :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}]
           (xt/q tu/*node* "SELECT _id, a, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))

(t/deftest update-null-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a 1}]])
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = NULL WHERE _id = 1"])
  (tu/flush-block! tu/*node*)
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = NULL WHERE _id = 1"])
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 2 WHERE _id = 1"])

  (t/is (= [{:xt/id 1, :a 1,          
             :xt/valid-from #xt/zdt "2020-01-01T00:00Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02T00:00Z[UTC]"}
            {:xt/id 1,
             :xt/valid-from #xt/zdt "2020-01-02T00:00Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-04T00:00Z[UTC]"}
            {:xt/id 1, :a 2,
             :xt/valid-from #xt/zdt "2020-01-04T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _valid_from"))))

(t/deftest update-for-portion-of-idempotency-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :xt/valid-from #xt/instant "2020-01-01Z", :a 1}
                             {:xt/id 1, :xt/valid-from #xt/instant "2020-01-03Z", :a 1.0}]])

  (xt/execute-tx tu/*node* ["UPDATE docs FOR VALID_TIME FROM TIMESTAMP '2020-01-02Z' TO TIMESTAMP '2020-01-04Z' SET a = 1 WHERE _id = 1"])

  (t/is (= [{:xt/id 1, :a 1,
             :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-03Z[UTC]"}
            {:xt/id 1, :a 1.0,
             :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/system-to #xt/zdt "2020-01-02Z[UTC]",
             :xt/valid-from #xt/zdt "2020-01-03Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-04Z[UTC]"}
            {:xt/id 1, :a 1.0,
             :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-from #xt/zdt "2020-01-04Z[UTC]"}
            {:xt/id 1, :a 1,          
             :xt/system-from #xt/zdt "2020-01-02Z[UTC]",
             :xt/valid-from #xt/zdt "2020-01-03Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-04Z[UTC]"}]
           (xt/q tu/*node* (->> ["SELECT *, _system_from, _system_to, _valid_from, _valid_to"
                                 "FROM docs FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"
                                 "ORDER BY _system_from, _valid_from"]
                                (str/join "\n"))))))

(t/deftest update-bulk-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :a 1}
                             {:xt/id 2, :a 2}
                             {:xt/id 3, :a 3}]])

  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 1"])

  (t/is (= [{:xt/id 1, :a 1 
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}          
            {:xt/id 2, :a 2,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 2, :a 1,
             :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 3, :a 3,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 3, :a 1,
             :xt/valid-from #xt/zdt "2020-01-02Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))

(t/deftest update-bulk-5030
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :a 1}
                             {:xt/id 2, :a 2}
                             {:xt/id 3, :a 3}]])

  (xt/execute-tx tu/*node* ["UPDATE docs SET a = a + 0"])
  (xt/execute-tx tu/*node* ["UPDATE docs SET a = a + 1 WHERE _id = 3"])

  (t/is (= [{:xt/id 1, :a 1 
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}          
            {:xt/id 2, :a 2,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]"}
            {:xt/id 3, :a 3,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-03Z[UTC]"}
            {:xt/id 3, :a 4,
             :xt/valid-from #xt/zdt "2020-01-03Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))

(t/deftest test-after-compaction
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a 1}]])
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a 2}]])
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 2, :a 3}]])

  (tu/flush-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (xt/execute-tx tu/*node* ["UPDATE docs FOR ALL VALID_TIME SET a = 2"])
  (t/is (= [{:xt/id 1, :a 1,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/system-to #xt/zdt "2020-01-04Z[UTC]"}
            {:xt/id 1, :a 2,
             :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-02Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-04Z[UTC]"}
            {:xt/id 1, :a 1,
             :xt/valid-from #xt/zdt "2020-01-02Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
             :xt/system-to #xt/zdt "2020-01-02Z[UTC]"}
            {:xt/id 1, :a 2,
             :xt/valid-from #xt/zdt "2020-01-02Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-02Z[UTC]"}

            {:xt/id 2, :a 3,
             :xt/valid-from #xt/zdt "2020-01-03Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-03Z[UTC]",
             :xt/system-to #xt/zdt "2020-01-04Z[UTC]"}
            {:xt/id 2, :a 2,
             :xt/valid-from #xt/zdt "2020-01-03Z[UTC]",
             :xt/system-from #xt/zdt "2020-01-04Z[UTC]"}]
           (xt/q tu/*node* (->> ["SELECT *, _valid_from, _valid_to, _system_from, _system_to"
                                 "FROM docs FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"
                                 "ORDER BY _id, _valid_from"]
                                (str/join "\n"))))))
