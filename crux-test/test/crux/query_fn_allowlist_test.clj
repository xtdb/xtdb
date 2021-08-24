(ns crux.query-fn-allowlist-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.query :as query]
            [crux.fixtures :as fix :refer [*api*]]))

(defn multiple-of-three? [n]
  (= 0 (mod n 3)))

(defn multiple-of-seven? [n]
  (= 0 (mod n 7)))

(t/deftest test-default-allow-list
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:xt/query-engine {:fn-allow-list '[]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
        (t/testing "using allowed default functions"
          (= #{[21]}
             (api/q
              (api/db *api*)
              '{:find [age]
                :in [[age ...]]
                :where [[(odd? age)]]}
              [21 22])))
        (t/testing "using disallowed default functions"
          (t/is
           (thrown-with-msg?
            IllegalArgumentException
            #"Query used a function that was not in the allowlist"
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(> age 21)] [(spit "crux.txt" age)]]}
             [21 22]))))
        (t/testing "using non clojure.core functions"
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

(t/deftest test-allowed-fns
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:xt/query-engine {:fn-allow-list '[crux.query-fn-allowlist-test/multiple-of-seven?]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
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
           [21 22])))))))

(t/deftest test-allowed-ns
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:xt/query-engine {:fn-allow-list '[crux.query-fn-allowlist-test]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
        (t/testing "disallowed namespace, non default fn"
          (t/is
           (thrown-with-msg?
            IllegalArgumentException
            #"Query used a function that was not in the allowlist"
            (api/q
             (api/db *api*)
             '{:find [age]
               :in [[age ...]]
               :where [[(clojure.string/capitalize age)]]}
             [21 22]))))
        (t/testing "allowed namespace"
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
               [21 22]))))))))

(t/deftest test-allowlist-strings
  (let [with-fixtures (t/join-fixtures
                       [(fix/with-opts {:xt/query-engine {:fn-allow-list ["crux.query-fn-allowlist-test/multiple-of-seven?"]}})
                        fix/with-node])]
    (with-fixtures
      (fn []
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
           [21 22])))))))
