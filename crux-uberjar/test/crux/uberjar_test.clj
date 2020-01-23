(ns crux.uberjar-test
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [crux.fixtures :as f]
            [clojure.tools.logging :as log]
            [crux.io :as cio])
  (:import (java.io File)))

(def ^File working-directory
  (.. (io/as-file (io/resource "crux/uberjar_test.clj"))
      getParentFile
      getParentFile
      getParentFile))

(defn build-uberjar []
  (log/info "building uberjar...")

  (let [{:keys [exit out] :as res} (sh/sh "lein" "with-profiles" "+uberjar-test" "uberjar"
                                          :dir working-directory)]
    (when-not (zero? exit)
      (throw (ex-info "lein uberjar exited with non-zero exit code" res))))

  (log/info "built uberjar, starting server..."))

(defn- string-array ^"[Ljava.lang.String;" [& strs]
  (into-array String strs))

(t/deftest test-uberjar-can-start
  (f/with-tmp-dir "uberjar" [uberjar-dir]
    (build-uberjar)

    (let [opts {:crux.node/topology 'crux.standalone/topology
                :crux.standalone/event-log-dir (str (io/file uberjar-dir "event-log"))
                :crux.kv/db-dir (str (io/file uberjar-dir "db-dir"))}

          process (.. (ProcessBuilder. (string-array
                                        "timeout" "30s"
                                        "java" "-jar" "target/crux-test-uberjar.jar"
                                        "-x" (pr-str opts)
                                        "-s" (str (cio/free-port))))
                      (directory working-directory)
                      start)]
      (try
        (with-open [out (io/reader (.getInputStream process))]
          (let [started-line? #(str/includes? % "org.eclipse.jetty.server.Server - Started")
                out-lines (->> (line-seq out)
                               (map #(doto % println)))
                _ (or (t/is (->> out-lines (filter started-line?) first))
                      (println (slurp (.getErrorStream process))))
                results (->> out-lines
                             (take-while (complement started-line?))
                             (str/join "\n"))]

            (t/testing "Crux version"
              (t/is (str/includes? results "Crux version:")))

            (t/testing "Options loaded"
              (t/is (str/includes? results "options:")))

            (t/testing "Options presented"
              (t/is (str/includes? results ":server-port")))))
        (finally
          (.destroy process))))))
