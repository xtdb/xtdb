(ns xtdb.docs.xtdb-http
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.string :as str]
            [xtdb.test-util :as tu]
            [clojure.java.shell :refer [sh]]))

(t/use-fixtures :each tu/with-mock-clock tu/with-http-client-node)

(defn load-test-file [name]
  (slurp (str "./src/test/resources/docs/" name)))

(defn script [name port]
  (-> (str name ".sh")
      load-test-file
      (str/replace "3000" (str port))))

(defn strip-comments [s]
  (->> s
       (str/split-lines)
       (remove #(str/starts-with? % "# "))
       (str/join "\n")))

(defn expected-output [name]
  (-> (str name ".expected-output.txt")
      load-test-file
      strip-comments
      str/trim))

(defn exec-output [script]
  (-> (sh "sh" "-c" script)
      :out
      str/trim))

(defn test-script [name]
  (t/is (= (expected-output name)
           (-> name
               (script tu/*http-port*)
               exec-output))))

(deftest test-guide
  (test-script "xtdb_http"))
