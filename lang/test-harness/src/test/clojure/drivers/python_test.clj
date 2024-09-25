(ns drivers.python-test
  (:require [clojure.test :refer [deftest use-fixtures is]]
            [test-harness.test-utils :as tu]
            [clojure.java.shell :refer [sh]]
            [xtdb.node :as xtn]))

(def project-root (str @tu/root-path "/lang/python/"))

(defn poetry-install [f]
  ;; Install dependencies
  (let [out (sh "poetry" "install" :dir project-root)]
    (println (:out out)))
  (f))

(def ^:dynamic ^long *pg-port* -1)

(defn- with-node [f]
  (with-open [node (xtn/start-node {:pgwire-server {:port 0}})]
    (binding [*pg-port* (.getPgPort node)]
      (f))))

(use-fixtures :once poetry-install)

(deftest python-http-test
  (tu/with-system
    #(let [out (sh "poetry"
                   "run" "pytest"
                   "--ignore=xtdb/test/test_pgwire.py"
                   (str "--junitxml=" project-root "/build/test-results/test/TEST-python-http-test.xml")
                   :dir project-root)]
       (is (= 0 (:exit out))
           (:out out)))))

(deftest python-pgwire-test
  (with-node #(let [out (sh "poetry"
                            "run" "pytest"
                            (str "--junitxml=" project-root "/build/test-results/test/TEST-python-pgwire-test.xml")
                            "xtdb/test/test_pgwire.py"
                            :dir project-root
                            :env (into {"PG_PORT" *pg-port*} (System/getenv)))]
                (is (= 0 (:exit out))
                    (:out out)))))
