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

(t/deftest redundant-pipeline
  (t/is (contains? (finding-types '(-> (from :docs [xt/id])))
                   :xtql/redundant-pipeline)))

(t/deftest redundant-unify
  (t/is (contains? (finding-types '(unify (from :docs [xt/id])))
                   :xtql/redundant-unify))
  (t/is (contains? (finding-types '(unify (rel [{:a 1} {:a 2}] [a])))
                   :xtql/redundant-unify)))

(t/deftest unrecognized-operation
  (t/testing "non existing operations"
    (t/is (contains? (finding-types '(my-op 1))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (my-op 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (my-op 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (my-op 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                            (my-op 1)))
                     :xtql/unrecognized-operation)))

  (t/testing "ops in wrong positions"
    (t/is (contains? (finding-types '(limit 1))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                         (from :other [xt/id])))
                     :xtql/unrecognized-operation)))

  (t/testing "unify clauses"
    (t/is (= (finding-types '(unify (from :t1 [xt/id])
                                    (from :t2 [xt/id])))
             #{}))
    (t/is (contains? (finding-types '(unify (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                            (limit 1)))
                     :xtql/unrecognized-operation))
    (t/is (contains? (finding-types '(-> (unify (limit 1))
                                         (limit 1)))
                     :xtql/unrecognized-operation))))

(t/deftest type-mismatch
  (t/testing "pipeline"
    (t/testing "all operations must be lists"
      (t/is (= (finding-types '(-> (from :docs [xt/id])
                                   (limit 1)))
               #{}))
      (t/is (contains? (finding-types '(-> :test))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(-> (from :docs [xt/id])
                                           :test))
                       :xtql/type-mismatch))))

  (t/testing "unify"
    (t/testing "all operations must be lists"
      (t/is (= (finding-types '(unify (from :t1 [xt/id])
                                      (from :t2 [xt/id])))
               #{}))
      (t/is (contains? (finding-types '(unify :test))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(unify (from :docs [xt/id])
                                              :test))
                       :xtql/type-mismatch))))

  (t/testing "from"
    (t/testing "table must be a keyword"
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
                       :xtql/type-mismatch))))

  (t/testing "binding"
    (t/testing "bindings must be a symbol or map"
      (t/is (contains? (finding-types '(from :docs [:test]))
                       :xtql/type-mismatch))
      (t/is (contains? (finding-types '(from :docs [:test]))
                       :xtql/type-mismatch)))

    (t/testing "map bindings must contain only keywords"
      (t/testing "qualified keywords work"
        (t/is (= (finding-types '(from :docs [{:xt/id 1}]))
                 #{})))
      (t/is (contains? (finding-types '(from :docs [{not-kw 1}]))
                       :xtql/type-mismatch)))))

(t/deftest unrecognized-parameter
  (t/testing "from"
    (t/is (contains? (finding-types '(from :docs {:test 1}))
                     :xtql/unrecognized-parameter))
    (t/is (contains? (finding-types '(from :docs [$xt/id]))
                     :xtql/unrecognized-parameter))))

(t/deftest missing-parameter
  (t/testing "from"
    (t/is (contains? (finding-types '(from :docs {}))
                     :xtql/missing-parameter))))
