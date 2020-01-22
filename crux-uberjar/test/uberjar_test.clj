(ns uberjar-test
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [crux.fixtures :as f]))

;; This is a hacky way to allow the uberjar tests to be ran from a directory other than crux-uberjar - typically the repo root.
(def working-directory
  (if (str/ends-with? (.getCanonicalPath (clojure.java.io/file ".")) "/crux-uberjar")
    "."
    "./crux-uberjar"))

(t/deftest test-uberjar-can-start
  (f/with-tmp-dir "uberjar" [uberjar-dir]
    (let [uberjar (let [{:keys [exit out] :as res} (sh/sh "lein" "uberjar"
                                                          :dir working-directory)]
                    (if (zero? exit)
                      out
                      (throw (ex-info "lein uberjar exited with non-zero exit code" res))))

          results (:out (sh "timeout" "10s"
                            "java" "-jar" uberjar
                            "-x" (pr-str {:crux.node/topology 'crux.standalone/topology
                                          :crux.standalone/event-log-dir (str (io/file uberjar-dir "event-log"))
                                          :crux.kv/db-dir (str (io/file uberjar-dir "db-dir"))})
                            :dir working-directory))]

      (t/testing "Results exist"
        (t/is (string? results)))

      (t/testing "Crux version"
        (t/is (str/includes? results "Crux version:")))

      (t/testing "Options loaded"
        (t/is (str/includes? results "options:")))

      (t/testing "Options presented"
        (t/is (str/includes? results ":server-port")))

      (t/testing "Server started"
        (t/is (str/includes? results "org.eclipse.jetty.server.Server - Started"))))))
