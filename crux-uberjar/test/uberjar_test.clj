(ns uberjar-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]))

(t/deftest test-uberjar-can-start
  (let [_ (sh/sh "test/test-uberjar.sh")
        file "uberjar-test-results"
        results (slurp file)]

    (t/testing "Results exist"
      (t/is (string? results)))

    (t/testing "Crux version"
      (t/is (.contains results "Crux version:")))

    (t/testing "Options loaded"
      (t/is (.contains results "options:")))

    (t/testing "Options presented"
      (t/is (and (.contains results ":db-dir")
                 (.contains results ":server-port")
                 (.contains results ":kv-backend"))))

    ;; Not sure how this will stand the test of time - joa
    (t/testing "Kafka attempted"
      (t/is (.contains results "kafka-producer")))

    (io/delete-file file)))

