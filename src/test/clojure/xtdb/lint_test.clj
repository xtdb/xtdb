(ns xtdb.lint-test
  (:require [clojure.test :as t]
            [clj-kondo.hooks-api :as api]
            [clj-kondo.core :as clj-kondo]))

(defn lint [s]
  (binding [;; Force hooks to reload
            api/*reload* true]
    (with-in-str s
      (clj-kondo/run!
        {:lint [;; Lint from stdin
                "-"]}))))

(defn query->src [q]
  (str "(require '[xtdb.api :as xt])"
       "(xt/q 'node '" q ")"))

(defn findings [q]
  (-> q query->src lint :findings))

(comment
  (findings '(unify (my-op 1))))

(defn finding-types [q]
  (->> (findings q)
       (map :type)
       (into #{})))

;; TODO: Also run queries through the xt parser to test it's valid

(t/deftest top-level-query
  (t/testing "top level must be pipeline or valid source op"
    (t/testing "invalid source op"
      (t/is (contains? (finding-types '(my-op 1))
                       :xtql/unrecognized-operation)))
    (t/testing "tail op in wrong position"
      (t/is (contains? (finding-types '(limit 1))
                       :xtql/unrecognized-operation)))))

(t/deftest pipeline
  (t/testing "all operations must be lists"
    (t/is (= (finding-types '(-> (from :docs [xt/id])
                                 (limit 1)))
             #{}))
    (t/is (contains? (finding-types '(-> :test))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         :test))
                     :xtql/type-mismatch)))

  (t/testing "redundant pipeline"
    (t/is (= (finding-types '(-> (from :docs [xt/id])
                                 (limit 10)))
             #{}))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])))
                     :xtql/redundant-pipeline)))

  (t/testing "non existant operations"
    (t/is (contains? (finding-types '(-> (my-op 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (my-op 1)))
                     :xtql/unrecognized-operation)))

  (t/testing "ops in wrong positions"
    (t/is (contains? (finding-types '(-> (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (from :other [xt/id])))
                     :xtql/unrecognized-operation))))

(t/deftest unify
  (t/testing "all operations must be lists"
    (t/is (= (finding-types '(unify (from :t1 [xt/id])
                                    (from :t2 [xt/id])))
             #{}))
    (t/is (contains? (finding-types '(unify :test))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                            :test))
                     :xtql/type-mismatch)))

  (t/testing "redundant unify"
    (t/is (= (finding-types '(unify (join (from :docs [xt/id])
                                          [xt/id])))
             #{}))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])))
                     :xtql/redundant-unify))
    (t/is (contains? (finding-types '(unify (rel [{:a 1} {:a 2}] [a])))
                     :xtql/redundant-unify)))

  (t/testing "non existing operations"
    (t/is (contains? (finding-types '(unify (my-op 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                            (my-op 1)))
                     :xtql/unrecognized-operation)))

  (t/testing "ops in wrong positions"
    (t/is (= (finding-types '(unify (from :t1 [xt/id])
                                    (from :t2 [xt/id])))
             #{}))
    (t/is (contains? (finding-types '(unify (foo)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                            (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (unify (limit 1))
                                         (limit 1)))
                     :xtql/unrecognized-operation))))

(t/deftest from
  (t/testing "table must be a keyword"
    (t/is (= (finding-types '(from :docs [xt/id]))
             #{}))
    (t/is (contains? (finding-types '(from docs [xt/id]))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(from 1 [xt/id]))
                     :xtql/type-mismatch)))

  (t/testing "opts must be a vector or map"
    (t/is (= #{} (finding-types '(from :docs [xt/id]))))
    (t/is (= #{} (finding-types '(from :docs {:bind [xt/id]}))))
    (t/is (contains? (finding-types '(from :docs xt/id))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(from :docs {:bind xt/id}))
                     :xtql/type-mismatch)))

  (t/testing "bindings"
    (t/testing "must be a symbol or map"
      (t/is (= (finding-types '(from :docs [test]))
               #{}))
      (t/is (= (finding-types '(from :docs [{:test 1}]))
               #{}))
      (t/is (= (finding-types '(from :docs {:bind [test]}))
               #{}))
      (t/is (= (finding-types '(from :docs {:bind [{:test 1}]}))
               #{}))
      (t/is (contains? (finding-types '(from :docs [:test]))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(from :docs {:bind [:test]}))
                       :xtql/type-mismatch)))

    (t/testing "symbols should not be argument variables"
      (t/is (contains? (finding-types '(from :docs [$xt/id]))
                       :xtql/unrecognized-parameter)))

    (t/testing "map bindings must contain only keywords"
      (t/is (= (finding-types '(from :docs [{:name "jim"}]))
               #{}))
      (t/testing "qualified keywords work"
        (t/is (= (finding-types '(from :docs [{:xt/id 1}]))
                 #{})))
      (t/is (contains? (finding-types '(from :docs [{not-kw 1}]))
                       :xtql/type-mismatch))))

  (t/testing "map opts"
    (t/testing "missing :bind"
      (t/is (contains? (finding-types '(from :docs {}))
                       :xtql/missing-parameter)))

    (t/testing "must contain only keywords"
      (t/is (contains? (finding-types '(from :docs {:bind [name]
                                                    not-kw 1}))
                       :xtql/type-mismatch)))

    (t/testing "unrecognized parameter"
      (t/is (= (finding-types '(from :docs {:bind [test]
                                            :for-valid-time (at #"2020")
                                            :for-system-time (at #"2020")}))
               #{}))
      (t/is (contains? (finding-types '(from :docs {:not-valid 1}))
                       :xtql/unrecognized-parameter)))))

(t/deftest rel
  (t/testing "bindings"
    (t/testing "must be a vector"
      (t/is (= (finding-types '(rel $t [a b c]))
               #{}))
      (t/is (contains? (finding-types '(rel $t {:a b}))
                       :xtql/type-mismatch)))

    (t/testing "must be a symbol or map"
      (t/is (= (finding-types '(rel $t [test]))
               #{}))
      (t/is (= (finding-types '(rel $t [{:test 1}]))
               #{}))
      (t/is (contains? (finding-types '(rel $t [:test]))
                       :xtql/type-mismatch)))

    (t/testing "symbols should not be argument variables"
      (t/is (contains? (finding-types '(rel $t [$xt/id]))
                       :xtql/unrecognized-parameter)))

    (t/testing "map bindings must contain only keywords"
      (t/is (= (finding-types '(rel $t [{:name "jim"}]))
               #{}))
      (t/testing "qualified keywords work"
        (t/is (= (finding-types '(rel $t [{:xt/id 1}]))
                 #{})))
      (t/is (contains? (finding-types '(rel $t [{not-kw 1}]))
                       :xtql/type-mismatch)))))

(t/deftest order-by
  (t/testing "opts must be a symbol or map"
    (t/is (= #{} (finding-types '(-> (from :docs [xt/id name])
                                     (order-by {:val xt/id
                                                :dir :asc
                                                :nulls :first}
                                               name)))))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id name])
                                         (order-by :xt/id name)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (order-by [xt/id name])))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (order-by {:val :xt/id})))
                     :xtql/type-mismatch)))

  (t/testing "symbols should not be argument variables"
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (order-by $something)))
                     :xtql/unrecognized-parameter)))

  (t/testing "map opts"
    (t/testing "missing :val"
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:dir :desc})))
                       :xtql/missing-parameter)))

    (t/testing "must contain only keywords"
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:val xt/id
                                                      not-kw 1})))
                       :xtql/type-mismatch)))

    (t/testing ":dir & :nulls have correct types"
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:val xt/id
                                                      :dir x})))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:val xt/id
                                                      :dir :x})))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:val xt/id
                                                      :nulls x})))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:val xt/id
                                                      :nulls :x})))
                       :xtql/type-mismatch)))

    (t/testing "unrecognized parameter"
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (order-by {:something xt/id})))
                       :xtql/unrecognized-parameter)))))

(t/deftest limit
  (t/testing "number of args"
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (limit)))
                     :xtql/invalid-arity))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (limit 1 1)))
                     :xtql/invalid-arity)))

  (t/testing "opt must be an integer"
    (t/is (= (finding-types '(-> (from :docs [xt/id])
                                 (limit 10)))
             #{}))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (limit 1.1)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (limit :test)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (limit "1.1")))
                     :xtql/type-mismatch))))

(t/deftest offset
  (t/testing "number of args"
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (offset)))
                     :xtql/invalid-arity))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (offset 1 1)))
                     :xtql/invalid-arity)))

  (t/testing "opt must be an integer"
    (t/is (= (finding-types '(-> (from :docs [xt/id])
                                 (offset 10)))
             #{}))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (offset 1.1)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (offset :test)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (offset "1.1")))
                     :xtql/type-mismatch))))

(t/deftest with
  (t/testing "tail operator"
    (t/testing "number of args"
      (t/is (= (finding-types '(-> (from :docs [xt/id])
                                   (with a b)))
               #{}))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (with)))
                       :xtql/invalid-arity)))

    (t/testing "opts must be either symbols or maps"
      (t/is (= (finding-types '(-> (from :docs [xt/id])
                                   (with a {:b xt/id})))
               #{}))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (with :test)))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (with [a {:b xt/id}])))
                       :xtql/type-mismatch)))

    (t/testing "symbols must not be argument variables"
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (with $something)))
                       :xtql/unrecognized-parameter)))

    (t/testing "map opts"
      (t/testing "all keys must be keywords"
        (t/is (= (finding-types '(-> (from :docs [xt/id])
                                     (with {:a xt/id} {:b xt/id})))
                 #{}))
        (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                             (with {not-kw xt/id})))
                         :xtql/type-mismatch))
        (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                             (with {1 xt/id})))
                         :xtql/type-mismatch)))))

  (t/testing "unify clause"
    (t/testing "number of args"
      (t/is (= (finding-types '(unify (from :docs [xt/id])
                                      (with {a xt/id} {b xt/id})))
               #{}))
      (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                              (with)))
                       :xtql/invalid-arity)))

    (t/testing "opts must be maps"
      (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                              (with test)))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                              (with :test)))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                              (with [a {:b xt/id}])))
                       :xtql/type-mismatch)))

    (t/testing "map opts"
      (t/testing "all keys must be symbols"
        (t/is (= (finding-types '(unify (from :docs [xt/id])
                                        (with {a xt/id} {b xt/id})))
                 #{}))
        (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                                (with {:kw xt/id})))
                         :xtql/type-mismatch))
        (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                                (with {1 xt/id})))
                         :xtql/type-mismatch))))))

(t/deftest return
  (t/is (= (finding-types '(-> (from :docs [xt/id])
                               (return a b)))
           #{}))
  (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                       (return)))
                   :xtql/invalid-arity))

  (t/testing "opts must be either symbols or maps"
    (t/is (= (finding-types '(-> (from :docs [xt/id])
                                 (return a {:b xt/id})))
             #{}))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (return :test)))
                     :xtql/type-mismatch))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (return [a {:b xt/id}])))
                     :xtql/type-mismatch)))

  (t/testing "symbols must not be argument variables"
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (return $something)))
                     :xtql/unrecognized-parameter)))

  (t/testing "map opts"
    (t/testing "all keys must be keywords"
      (t/is (= (finding-types '(-> (from :docs [xt/id])
                                   (return {:a xt/id} {:b xt/id})))
               #{}))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (return {not-kw xt/id})))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           (return {1 xt/id})))
                       :xtql/type-mismatch)))))
