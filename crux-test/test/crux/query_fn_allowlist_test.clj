(ns crux.query-fn-allowlist-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as fix :refer [*api*]]))

(defn multiple-of-three? [n]
  (= 0 (mod n 3)))

(defn multiple-of-seven? [n]
  (= 0 (mod n 7)))

(t/deftest test-allowed-fns
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:crux/query-engine {:fn-allow-list '[clojure.core/odd?
                                                                             crux.query-fn-allowlist-test/multiple-of-seven?]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
        (t/testing "with unqualified symbols / clojure.core functions"
          (t/is
           (= #{[21]}
              (api/q
               (api/db *api*)
               '{:find [age]
                 :in [[age ...]]
                 :where [[(odd? age)]]}
               [21 22])))
          (t/is
           (thrown-with-msg?
            IllegalArgumentException
            #"Query used a function that was not in the allowlist"
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(even? age)]]}
             [21 22]))))
        (t/testing "with qualified symbols"
          (t/is
           (= #{[21]}
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(crux.query-fn-allowlist-test/multiple-of-seven? age)]]}
             [21 22])))
          (t/is
           (thrown-with-msg?
            IllegalArgumentException
            #"Query used a function that was not in the allowlist"
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(crux.query-fn-allowlist-test/multiple-of-three? age)]]}
             [21 22]))))))))

(t/deftest test-allowed-ns
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:crux/query-engine {:fn-allow-list '[crux.query-fn-allowlist-test]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
        (t/is
         (thrown-with-msg?
          IllegalArgumentException
          #"Query used a function that was not in the allowlist"
          (api/q
           (api/db *api*)
           '{:find [age]
             :in [[age ...]]
             :where [[(even? age)]]}
           [21 22])))
        (t/is
         (thrown-with-msg?
          IllegalArgumentException
          #"Query used a function that was not in the allowlist"
          (api/q
           (api/db *api*)
           '{:find [age]
             :in [[age ...]]
             :where [[(odd? age)]]}
           [21 22])))
        (t/is
         (= #{[21]}
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(crux.query-fn-allowlist-test/multiple-of-three? age)]]}
             [21 22])))
        (t/is
         (= #{[21]}
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(crux.query-fn-allowlist-test/multiple-of-seven? age)]]}
             [21 22])))))))
