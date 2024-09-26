(ns xtdb.langs-test
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.test :as t]
            [xtdb.pgwire :as pgw]))

(def project-root
  (loop [dir (io/file ".")]
    (assert dir (str ".git not found in parents of " (.getCanonicalFile (io/file "."))))
    (if (.exists (io/file dir ".git"))
      (.getCanonicalFile dir)
      (recur (.getParentFile dir)))))

(def ^:dynamic ^long *pg-port* -1)

(defn- with-node [f]
  (with-open [srv (pgw/open-playground)]
    (binding [*pg-port* (:port srv)]
      (f))))

(t/use-fixtures :each with-node)

(defn yarn-install [dir]
  ;; `--frozen-lockfile` apparently becomes `--immutable` in yarn 2
  (let [out (sh "yarn" "install" "--frozen-lockfile" :dir dir)]
    (println (:out out))
    (binding [*out* *err*]
      (println (:err out)))))

(t/deftest js-test
  (let [js-dir (io/file project-root "lang/js")]
    (yarn-install js-dir)

    (let [out (sh "yarn" "run" "test"
                  "--reporter" "mocha-junit-reporter"
                  "--reporterOptions" (str "mochaFile=" project-root "/build/test-results/test/TEST-js-test.xml")
                  :dir js-dir
                  :env (into {"PG_PORT" *pg-port*} (System/getenv)))]
      (println (:out out))
      (binding [*out* *err*]
        (println (:err out)))
      (t/is (= 0 (:exit out))))))

(def py-report-path
  (str project-root "/build/test-results/test/TEST-python-test.xml"))

(defn poetry-install [dir]
  ;; Install dependencies
  (let [out (sh "poetry" "install" :dir dir)]
    (println (:out out))))

(t/deftest python-test
  (let [py-dir (io/file project-root "lang/python")]
    (poetry-install py-dir)

    (let [out (sh "poetry" "run" "pytest"
                  (str "--junitxml=" py-report-path)
                  :dir py-dir
                  :env (into {"PG_PORT" *pg-port*} (System/getenv)))]
      (t/is (= 0 (:exit out))
            (:out out)))))
