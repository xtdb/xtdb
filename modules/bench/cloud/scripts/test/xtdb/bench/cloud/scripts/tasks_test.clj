(ns xtdb.bench.cloud.scripts.tasks-test
  (:require [clojure.test :refer [deftest is testing]]
            [xtdb.bench.cloud.scripts.tasks :as tasks]
            [clojure.string :as str]))

(deftest format-duration-test
  (testing "format-duration with various units"
    (is (= "1.0h" (tasks/format-duration :nanos 3600e9)))
    (is (= "1.5h" (tasks/format-duration :nanos 5400e9)))
    (is (= "30.0m" (tasks/format-duration :nanos 1800e9)))
    (is (= "1.50s" (tasks/format-duration :nanos 1500e6)))
    (is (= "500ms" (tasks/format-duration :nanos 500e6)))
    (is (= "250µs" (tasks/format-duration :nanos 250e3)))
    (is (= "100ns" (tasks/format-duration :nanos 100))))

  (testing "format-duration with different input units"
    (is (= "1.0h" (tasks/format-duration :hours 1)))
    (is (= "1.0m" (tasks/format-duration :minutes 1)))
    (is (= "1.00s" (tasks/format-duration :seconds 1)))
    (is (= "1.00s" (tasks/format-duration :millis 1000)))
    (is (= "1ms" (tasks/format-duration :micros 1000))))

  (testing "format-duration with nil returns nil"
    (is (nil? (tasks/format-duration :millis nil)))))

(deftest title-case-test
  (testing "title-case conversion"
    (is (= "Simple Query" (tasks/title-case "simple-query")))
    (is (= "Multiple Word Query" (tasks/title-case "multiple-word-query")))
    (is (= "Single" (tasks/title-case "single")))
    (is (= "" (tasks/title-case "")))))

(deftest tpch-stage->query-row-test
  (testing "valid hot query stage"
    (let [stage {:stage "hot-queries-q1-pricing-summary"
                 :time-taken-ms 1500}
          result (tasks/tpch-stage->query-row 0 stage)]
      (is (= "Hot" (:temp result)))
      (is (= "Q1" (:q result)))
      (is (= "Pricing Summary" (:query-name result)))
      (is (= 1500 (:time-taken-ms result)))))

  (testing "valid cold query stage"
    (let [stage {:stage "cold-queries-q5-local-supplier"
                 :time-taken-ms 2000}
          result (tasks/tpch-stage->query-row 1 stage)]
      (is (= "Cold" (:temp result)))
      (is (= "Q5" (:q result)))
      (is (= "Local Supplier" (:query-name result)))))

  (testing "invalid stage returns nil"
    (is (nil? (tasks/tpch-stage->query-row 0 {:stage "ingest" :time-taken-ms 1000})))))

(deftest tpch-summary->query-rows-test
  (testing "calculating query rows with percentages"
    (let [summary {:query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                  {:stage "cold-queries-q2-min-cost" :time-taken-ms 3000}]}
          result (tasks/tpch-summary->query-rows summary)
          rows (:rows result)
          total (:total-ms result)]
      (is (= 4000 total))
      (is (= 2 (count rows)))
      (is (= "25.00%" (:percent-of-total (first rows))))
      (is (= "75.00%" (:percent-of-total (second rows))))))

  (testing "empty query stages"
    (let [summary {:query-stages []}
          result (tasks/tpch-summary->query-rows summary)]
      (is (= 0 (:total-ms result)))
      (is (empty? (:rows result))))))

(deftest normalize-format-test
  (testing "valid formats"
    (is (= :table (tasks/normalize-format :table)))
    (is (= :slack (tasks/normalize-format "slack")))
    (is (= :github (tasks/normalize-format "GITHUB")))
    (is (= :table (tasks/normalize-format "Table"))))

  (testing "invalid formats default to :table"
    (is (= :table (tasks/normalize-format :invalid)))
    (is (= :table (tasks/normalize-format "xyz")))
    (is (= :table (tasks/normalize-format nil)))))

(deftest parse-tpch-log-test
  (testing "parsing a valid tpch log"
    (let [temp-file (java.io.File/createTempFile "tpch-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"stage\":\"submit-docs\",\"time-taken-ms\":100}"
                         "{\"stage\":\"hot-queries-q1-pricing\",\"time-taken-ms\":1500}"
                         "{\"stage\":\"cold-queries-q2-min-cost\",\"time-taken-ms\":2500}"
                         "{\"stage\":\"sync\",\"time-taken-ms\":200}"
                         "{\"benchmark\":\"tpch\",\"time-taken-ms\":10000}"]))
        (let [result (tasks/parse-tpch-log (.getPath temp-file))]
          (is (= 4 (count (:all-stages result))))
          (is (= 2 (count (:query-stages result))))
          (is (= 10000 (:benchmark-total-time-ms result)))
          (is (= "hot-queries-q1-pricing" (:stage (first (:query-stages result))))))
        (finally
          (.delete temp-file))))))

(deftest parse-yakbench-log-test
  (testing "parsing a valid yakbench log"
    (let [temp-file (java.io.File/createTempFile "yakbench-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"profiles\":{\"profile1\":[{\"id\":\"q1\",\"mean\":1000000,\"p50\":900000,\"p90\":1100000,\"p99\":1200000,\"n\":100}]}}"
                         "{\"benchmark\":\"yakbench\",\"time-taken-ms\":5000}"]))
        (let [result (tasks/parse-yakbench-log (.getPath temp-file))]
          (is (contains? (:profiles result) :profile1))
          (is (= 5000 (:benchmark-total-time-ms result)))
          (is (= "q1" (:id (first (:profile1 (:profiles result)))))))
        (finally
          (.delete temp-file))))))

(deftest load-summary-test
  (testing "loading tpch summary"
    (let [temp-file (java.io.File/createTempFile "tpch-test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"hot-queries-q1-test\",\"time-taken-ms\":1000}")
        (let [result (tasks/load-summary "tpch" (.getPath temp-file))]
          (is (= "tpch" (:benchmark-type result)))
          (is (contains? result :query-stages)))
        (finally
          (.delete temp-file)))))

  (testing "unsupported benchmark type throws error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unsupported benchmark type"
                          (tasks/load-summary "invalid" "dummy.log")))))

(deftest summary->table-tpch-test
  (testing "tpch table format"
    (let [summary {:benchmark-type "tpch"
                   :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                  {:stage "cold-queries-q2-min-cost" :time-taken-ms 2000}]
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->table summary)]
      (is (string? result))
      (is (str/includes? result "Hot"))
      (is (str/includes? result "Q1"))
      (is (str/includes? result "Pricing"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->table-yakbench-test
  (testing "yakbench table format"
    (let [summary {:benchmark-type "yakbench"
                   :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                          :p90 1100000 :p99 1200000 :n 100 :sum 1000000}]}
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->table summary)]
      (is (string? result))
      (is (str/includes? result "profile1/q1"))
      (is (str/includes? result "percent-of-total"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty profiles still formats (validation happens earlier)"
    (let [summary {:benchmark-type "yakbench" :profiles {}}
          result (tasks/summary->table summary)]
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->slack-tpch-test
  (testing "tpch slack format wrapped in code blocks"
    (let [summary {:benchmark-type "tpch"
                   :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}]
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->slack summary)]
      (is (str/includes? result "```"))
      (is (str/ends-with? result "```"))
      (is (str/includes? result "Hot Q1 Pricing"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty query stages still formats (validation happens earlier)"
    (let [summary {:benchmark-type "tpch" :query-stages []}
          result (tasks/summary->slack summary)]
      (is (str/includes? result "```"))
      (is (str/includes? result "Total query time")))))

(deftest summary->slack-yakbench-test
  (testing "yakbench slack format"
    (let [summary {:benchmark-type "yakbench"
                   :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000 :p99 1200000 :n 100 :sum 1000000}]}
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->slack summary)]
      (is (str/includes? result "```"))
      (is (str/ends-with? result "```"))
      (is (str/includes? result "profile1/q1"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->github-markdown-tpch-test
  (testing "tpch github markdown format"
    (let [summary {:benchmark-type "tpch"
                   :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                  {:stage "cold-queries-q2-min-cost" :time-taken-ms 2000}]
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->github-markdown summary)]
      (is (str/includes? result "| Temp | Query |"))
      (is (str/includes? result "|"))
      (is (str/includes? result "| Hot | Q1 |"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty query stages still formats (validation happens earlier)"
    (let [summary {:benchmark-type "tpch" :query-stages []}
          result (tasks/summary->github-markdown summary)]
      (is (str/includes? result "Total query time")))))

(deftest summary->github-markdown-yakbench-test
  (testing "yakbench github markdown format"
    (let [summary {:benchmark-type "yakbench"
                   :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                          :p90 1100000 :p99 1200000 :n 100 :sum 1000000}]}
                   :benchmark-total-time-ms 5000}
          result (tasks/summary->github-markdown summary)]
      (is (str/includes? result "| Query | N | P50 |"))
      (is (str/includes? result "% of total"))
      (is (str/includes? result "| profile1/q1 |"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty profiles still formats (validation happens earlier)"
    (let [summary {:benchmark-type "yakbench" :profiles {}}
          result (tasks/summary->github-markdown summary)]
      (is (str/includes? result "Total query time")))))

(deftest yakbench-summary->query-rows-percent-test
  (testing "yakbench percent-of-total calculation"
    (let [summary {:profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                          :p90 1100000 :p99 1200000 :n 100 :sum 1000000}
                                         {:id "q2" :mean 3000000 :p50 2700000
                                          :p90 3300000 :p99 3600000 :n 50 :sum 3000000}]}}
          result (tasks/yakbench-summary->query-rows summary)
          rows (:rows result)
          total-ms (:total-ms result)]
      (is (= 2 (count rows)))
      (is (= "25.00%" (:percent-of-total (first rows))))
      (is (= "75.00%" (:percent-of-total (second rows))))
      ;; total should be 4000000 nanos = 4000 micros = 4 ms
      (is (= 4.0 total-ms)))))

(deftest render-summary-test
  (testing "rendering with different formats"
    (let [summary {:benchmark-type "tpch"
                   :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}]
                   :benchmark-total-time-ms 5000}]
      (is (string? (tasks/render-summary summary {:format :table})))
      (is (str/includes? (tasks/render-summary summary {:format :slack}) "```"))
      (is (str/includes? (tasks/render-summary summary {:format :github}) "|"))))

  (testing "default format is table"
    (let [summary {:benchmark-type "tpch" :query-stages []}
          result (tasks/render-summary summary {:format nil})]
      (is (str/includes? result "Total query time")))))

(deftest summarize-log-test
  (testing "summarize-log with valid args"
    (let [temp-file (java.io.File/createTempFile "test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"hot-queries-q1-test\",\"time-taken-ms\":1000}")
        (let [result (tasks/summarize-log ["tpch" (.getPath temp-file)])]
          (is (string? result))
          (is (str/includes? result "Hot"))
          (is (str/includes? result "Q1"))
          (is (str/includes? result "Total query time")))
        (finally
          (.delete temp-file)))))

  (testing "summarize-log with format option"
    (let [temp-file (java.io.File/createTempFile "test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"hot-queries-q1-test\",\"time-taken-ms\":1000}")
        (let [result (tasks/summarize-log ["--format" "slack" "tpch" (.getPath temp-file)])]
          (is (str/includes? result "```"))
          (is (str/includes? result "Total query time")))
        (finally
          (.delete temp-file)))))

  (testing "summarize-log without benchmark-type throws error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Benchmark type is required"
                          (tasks/summarize-log []))))

  (testing "summarize-log without log-file-path throws error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Log file path is required"
                          (tasks/summarize-log ["tpch"]))))

  (testing "summarize-log with extra args throws error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Too many positional arguments"
                          (tasks/summarize-log ["tpch" "log.txt" "extra"]))))

  (testing "summarize-log with empty tpch query stages throws error"
    (let [temp-file (java.io.File/createTempFile "test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"ingest\",\"time-taken-ms\":1000}")
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No query stages found"
                              (tasks/summarize-log ["tpch" (.getPath temp-file)])))
        (finally
          (.delete temp-file)))))

  (testing "summarize-log with empty yakbench profiles throws error"
    (let [temp-file (java.io.File/createTempFile "test" ".log")]
      (try
        (spit temp-file "{\"benchmark\":\"yakbench\",\"time-taken-ms\":5000}")
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No profile data found"
                              (tasks/summarize-log ["yakbench" (.getPath temp-file)])))
        (finally
          (.delete temp-file))))))

(deftest github-table-separator-test
  (testing "separator line has at least 3 dashes per column"
    (let [columns [{:key :name :header "Name"}
                   {:key :age :header "Age"}
                   {:key :score :header "Score"}]
          rows [{:name "Alice" :age 30 :score 95.5}
                {:name "Bob" :age 25 :score 87.3}]
          result (tasks/github-table columns rows)
          lines (str/split-lines result)
          separator-line (second lines)]
      (is (>= (count lines) 2) "Table should have at least header and separator lines")
      (let [separator-parts (str/split separator-line #"\|")
            ;; Remove empty strings from split (leading/trailing separators)
            separator-parts (filter #(not (str/blank? %)) separator-parts)]
        (doseq [part separator-parts]
          (let [dash-count (count (filter #(= % \-) part))]
            (is (>= dash-count 3)
                (format "Separator part '%s' should have at least 3 dashes, but has %d" part dash-count)))))))

  (testing "separator line handles short headers (minimum 3 dashes)"
    (let [columns [{:key :a :header "A"}
                   {:key :b :header "B"}]
          rows [{:a 1 :b 2}]
          result (tasks/github-table columns rows)
          lines (str/split-lines result)
          separator-line (second lines)
          separator-parts (filter #(not (str/blank? %)) (str/split separator-line #"\|"))]
      (doseq [part separator-parts]
        (is (>= (count (filter #(= % \-) part)) 3)
            (format "Even short headers should have at least 3 dashes in separator: '%s'" part)))))

  (testing "separator line matches header width for long headers"
    (let [columns [{:key :long :header "Very Long Column Header Name"}]
          rows [{:long "value"}]
          result (tasks/github-table columns rows)
          lines (str/split-lines result)
          header-line (first lines)
          separator-line (second lines)
          header-parts (filter #(not (str/blank? %)) (str/split header-line #"\|"))
          separator-parts (filter #(not (str/blank? %)) (str/split separator-line #"\|"))]
      (is (= (count header-parts) (count separator-parts))
          "Separator should have same number of columns as header")
      (doseq [[header-part separator-part] (map vector header-parts separator-parts)]
        (let [header-width (count (str/trim header-part))
              separator-width (count separator-part)]
          (is (>= separator-width header-width)
              (format "Separator width (%d) should be >= header width (%d) for '%s'"
                      separator-width header-width header-part))
          (is (>= separator-width 3)
              (format "Separator should have at least 3 dashes even if header is shorter")))))))
