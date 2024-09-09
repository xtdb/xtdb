(ns drivers.js-test
  (:require [clojure.java.shell :refer [sh]]
            [clojure.test :as t]
            [test-harness.test-utils :as tu]
            [xtdb.node :as xtn]
            [xtdb.pgwire.playground :as pgpg]))

(def project-root (str @tu/root-path "/lang/js/"))

(defn yarn-install [f]
  ;; `--frozen-lockfile` apparently becomes `--immutable` in yarn 2
  (let [out (sh "yarn" "install" "--frozen-lockfile" :dir project-root)]
    (println (:out out))
    (binding [*out* *err*]
      (println (:err out))))
  (f))

(def ^:dynamic ^long *pg-port* -1)

(defn- with-node [f]
  (with-open [node (xtn/start-node {:pgwire-server {:port 0}})]
    (binding [*pg-port* (.getPgPort node)]
      (f))))

(comment
  (do
    (require '[integrant.core :as ig]
             '[integrant.repl :as ir]
             '[xtdb.util :as util]
             '[xtdb.pgwire.playground :as pgpg])

    (defmethod ig/init-key ::playground [_ _]
      (pgpg/open-playground))

    (defmethod ig/halt-key! ::playground [_ playground]
      (util/close playground))

    (ir/set-prep! (fn []
                    {::playground {}})))

  (ir/halt)

  (ir/go)

  )

(t/use-fixtures :once yarn-install)
(t/use-fixtures :each with-node)

(t/deftest js-test
  (let [out (sh "yarn" "run" "test"
                "--reporter" "mocha-junit-reporter"
                "--reporterOptions" (str "mochaFile=" project-root "/build/test-results/test/TEST-js-test.xml")
                :dir project-root
                :env (into {"PG_PORT" *pg-port*} (System/getenv)))]
    (println (:out out))
    (binding [*out* *err*]
      (println (:err out)))
    (t/is (= 0 (:exit out)))))
