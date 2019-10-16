(ns uberjar-test
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]))

;; This is a hacky way to allow the uberjar tests to be ran from a directory other than crux-uberjar - typically crux-dev.
(def in-uberjar-directory?
  (let [active-directory (.getCanonicalPath (clojure.java.io/file "."))]
    (str/ends-with? active-directory "/crux-uberjar")))

(def uberjar-directory
  (if in-uberjar-directory?
    "."
    "../crux-uberjar"))

(def results-file
  (str uberjar-directory "/target/uberjar-test-results"))

(defn- sh-with-err-out [& args]
  (let [{:keys [exit err]} (apply sh/sh
                                  (concat args [:dir uberjar-directory]))]
    (when-not (zero? exit)
      (binding [*out* *err*]
        (println err)))))

(t/deftest test-uberjar-can-start
  (when (.exists (io/file results-file))
    (io/delete-file results-file))

  (let [_ (sh-with-err-out "chmod" "+x" "test/test-uberjar.sh")
        _ (sh-with-err-out "chmod" "+x" "test/test-start-uberjar.sh")
        _ (sh-with-err-out "test/test-uberjar.sh")
        results (slurp results-file)]
    (t/testing "Results exist"
      (t/is (string? results)))

    (t/testing "Crux version"
      (t/is (str/includes? results "Crux version:")))

    (t/testing "Options loaded"
      (t/is (str/includes? results "options:")))

    (t/testing "Options presented"
      (t/is (str/includes? results ":server-port")))

    ;; Not sure how this will stand the test of time - joa
    (t/testing "Kafka attempted"
      (t/is (str/includes? results "kafka-producer")))

    (io/delete-file results-file)))
