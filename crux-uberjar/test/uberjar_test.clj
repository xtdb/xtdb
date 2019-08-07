(ns uberjar-test
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]))

(def file "target/uberjar-test-results")

(defn- sh-with-err-out [& args]
  (let [{:keys [exit err]} (apply sh/sh args)]
    (when-not (zero? exit)
      (binding [*out* *err*]
        (println err)))))

(t/deftest test-uberjar-can-start
  (when (.exists (io/file file))
    (io/delete-file file))

  (let [_ (sh-with-err-out "chmod" "+x" "test/test-uberjar.sh")
        _ (sh-with-err-out "chmod" "+x" "test/test-start-uberjar.sh")
        _ (sh-with-err-out "test/test-uberjar.sh")
        results (slurp file)]

    (t/testing "Results exist"
      (t/is (string? results)))

    (t/testing "Crux version"
      (t/is (str/includes? results "Crux version:")))

    (t/testing "Options loaded"
      (t/is (str/includes? results "options:")))

    (t/testing "Options presented"
      (t/is (and (str/includes? results ":db-dir")
                 (str/includes? results ":server-port")
                 (str/includes? results ":kv-backend"))))

    ;; Not sure how this will stand the test of time - joa
    (t/testing "Kafka attempted"
      (t/is (str/includes? results "kafka-producer")))

    (io/delete-file file)))
