(ns drivers.python-test
  (:require [clojure.test :refer [deftest use-fixtures is]]
            [test-harness.test-utils :as tu]
            [clojure.java.shell :refer [sh]]))

(def project-root (str @tu/root-path "/lang/python/"))
(def report-path (str project-root "/build/test-results/test/TEST-python-test.xml"))

(defn poetry-install [f]
  ;; Install dependencies
  (let [out (sh "poetry" "install" :dir project-root)]
    (println (:out out)))
  (f))

(use-fixtures :once poetry-install)

(deftest python-test
  (let [out (sh "poetry"
                "run" "pytest"
                (str "--junitxml=" report-path)
                :dir project-root)]
    (is (= 0 (:exit out))
        (:out out))))
