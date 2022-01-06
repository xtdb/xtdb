(ns core2.operator.join-test
  (:require [clojure.test :as t]
            [core2.operator.join :as join]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.ICursor
           org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(def ^:private a-field (ty/->field "a" ty/bigint-type false))
(def ^:private b-field (ty/->field "b" ty/bigint-type false))
(def ^:private c-field (ty/->field "c" ty/bigint-type false))

(defn- run-join-test
  ([join-op left-blocks right-blocks]
   (run-join-test join-op left-blocks right-blocks {}))

  ([->join-cursor left-blocks right-blocks
    {:keys [left-fields right-fields left-join-col right-join-col]
     :or {left-fields [a-field], right-fields [b-field]
          left-join-col "a", right-join-col "b"}}]

   (with-open [left-cursor (tu/->cursor (Schema. left-fields) left-blocks)
               right-cursor (tu/->cursor (Schema. right-fields) right-blocks)
               ^ICursor join-cursor (->join-cursor tu/*allocator*
                                                   left-cursor left-join-col
                                                   right-cursor right-join-col)]

     (mapv set (tu/<-cursor join-cursor)))))

(t/deftest test-cross-join
  (letfn [(->cross-join-cursor [al lc _ljc rc _rjc]
            (join/->cross-join-cursor al lc rc))]
    (t/is (= [#{{:a 12, :b 10, :c 1}
                {:a 0, :b 10, :c 1}
                {:a 0, :b 15, :c 2}
                {:a 12, :b 15, :c 2}}
              #{{:a 100, :b 10, :c 1}
                {:a 100, :b 15, :c 2}}
              #{{:a 0, :b 83, :c 3}
                {:a 12, :b 83, :c 3}}
              #{{:a 100, :b 83, :c 3}}]
             (->> (run-join-test ->cross-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [[{:b 10 :c 1}, {:b 15 :c 2}]
                                  [{:b 83 :c 3}]]
                                 {:right-fields [b-field c-field]})
                  (mapv set))))

    (t/is (empty? (run-join-test ->cross-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 []))
          "empty input and output")))

(t/deftest test-equi-join
  (t/is (= [#{{:a 12, :b 12}}
            #{{:a 100, :b 100}
              {:a 0, :b 0}}]
           (->> (run-join-test join/->equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100} {:b 0}]])
                (mapv set))))

  (t/is (= [#{{:a 12}}
            #{{:a 100}, {:a 0}}]
           (->> (run-join-test join/->equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:a 12}, {:a 2}]
                                [{:a 100} {:a 0}]]
                               {:left-fields [a-field], :right-fields [a-field]
                                :left-join-col "a", :right-join-col "a"})
                (mapv set)))
        "same column name")

  (t/is (empty? (run-join-test join/->equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               []))
        "empty input")

  (t/is (empty? (run-join-test join/->equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 10 :c 1}, {:b 15 :c 2}]
                                [{:b 83 :c 3}]]))
        "empty output"))

(t/deftest test-semi-equi-join
  (t/is (= [#{{:a 12}} #{{:a 100}}]
           (->> (run-join-test join/->left-semi-equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]])
                (mapv set))))

  (t/testing "empty input"
    (t/is (empty? (run-join-test join/->left-semi-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])))

    (t/is (empty? (run-join-test join/->left-semi-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]])))

    (t/is (empty? (run-join-test join/->left-semi-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [[]]))))

  (t/is (empty? (run-join-test join/->left-semi-equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 10 :c 1}, {:b 15 :c 2}]
                                [{:b 83 :c 3}]]))
        "empty output"))

(t/deftest test-left-equi-join
  (t/is (= [#{{:a 12, :b 12, :c 0}, {:a 12, :b 12, :c 2}, {:a 0, :b nil, :c nil}}
            #{{:a 12, :b 12, :c 0}, {:a 12, :b 12, :c 2}, {:a 100, :b 100, :c 3}}]
           (->> (run-join-test join/->left-outer-equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 12}, {:a 100}]]
                               [[{:b 12, :c 0}, {:b 2, :c 1}]
                                [{:b 12, :c 2}, {:b 100, :c 3}]]
                               {:right-fields [b-field c-field]})
                (mapv set))))

  (t/testing "empty input"
    (t/is (= [#{{:a 12}, {:a 0}}
              #{{:a 100}}]
             (->> (run-join-test join/->left-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])
                  (mapv set))))

    (t/is (empty? (run-join-test join/->left-outer-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]])))

    ;; TODO weird that this includes `:b nil` but the above doesn't?
    ;; dynamic schema's likely always going to have these kinds of impacts
    ;; unless we could give ICursors knowledge of the columns, so that we at least get the same cols every time?
    ;; (we used to, but got rid of it because we didn't need it at the time)
    (t/is (= [#{{:a 12, :b nil}, {:a 0, :b nil}}
              #{{:a 100, :b nil}}]
             (->> (run-join-test join/->left-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [[]])
                  (mapv set))))))

(t/deftest test-anti-equi-join
  (t/is (= [#{{:a 0}}]
           (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]])
                (mapv set))))

  (t/testing "empty input"
    (t/is (empty? (run-join-test join/->left-anti-semi-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100}]])))

    (t/is (= [#{{:a 12}, {:a 0}}
              #{{:a 100}}]
             (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])
                  (mapv set)))))

  (t/is (empty? (run-join-test join/->left-anti-semi-equi-join-cursor
                               [[{:a 12}, {:a 2}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]]))
        "empty output"))
