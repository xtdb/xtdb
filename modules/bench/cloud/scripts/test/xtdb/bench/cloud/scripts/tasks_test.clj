(ns xtdb.bench.cloud.scripts.tasks-test
  (:require [clojure.test :refer [deftest is testing]]
            [xtdb.bench.cloud.scripts.tasks :as tasks]
            [xtdb.bench.cloud.scripts.log-parsing :as log-parsing]
            [xtdb.bench.cloud.scripts.summary :as summary]
            [xtdb.bench.cloud.scripts.util :as util]
            [xtdb.bench.cloud.scripts.k8s :as k8s]
            [babashka.process :as proc]
            [clojure.string :as str]))

(deftest parse-benchmark-summary-test
  (testing "extracts time-taken-ms from summary stage"
    (let [lines ["{\"stage\":\"summary\",\"time-taken-ms\":12345}"]
          result (log-parsing/parse-benchmark-summary lines)]
      (is (= 12345 (:benchmark-total-time-ms result)))
      (is (= "summary" (get-in result [:benchmark-summary :stage])))))

  (testing "ignores init stage with benchmark key, picks summary stage"
    (let [lines ["{\"stage\":\"init\",\"benchmark\":\"TPC-H (OLAP)\",\"system\":{\"jre\":\"Temurin-21\"}}"
                 "{\"stage\":\"hot-queries-q1\",\"time-taken-ms\":1000}"
                 "{\"stage\":\"summary\",\"time-taken-ms\":50000,\"benchmark\":\"TPC-H (OLAP)\"}"]
          result (log-parsing/parse-benchmark-summary lines)]
      (is (= 50000 (:benchmark-total-time-ms result)))
      (is (= "summary" (get-in result [:benchmark-summary :stage])))))

  (testing "returns nil when no summary stage exists"
    (let [lines ["{\"stage\":\"init\",\"benchmark\":\"TPC-H (OLAP)\"}"
                 "{\"stage\":\"hot-queries-q1\",\"time-taken-ms\":1000}"]
          result (log-parsing/parse-benchmark-summary lines)]
      (is (nil? (:benchmark-total-time-ms result)))
      (is (nil? (:benchmark-summary result)))))

  (testing "handles malformed JSON gracefully"
    (let [lines ["{\"stage\":\"summary\",\"time-taken-ms\":invalid}"]
          result (log-parsing/parse-benchmark-summary lines)]
      (is (nil? (:benchmark-total-time-ms result)))
      (is (nil? (:benchmark-summary result)))))

  (testing "handles empty lines"
    (let [result (log-parsing/parse-benchmark-summary [])]
      (is (nil? (:benchmark-total-time-ms result)))
      (is (nil? (:benchmark-summary result))))))

(deftest format-duration-test
  (testing "format-duration with various units"
    (is (= "1.0h" (util/format-duration :nanos 3600e9)))
    (is (= "1.5h" (util/format-duration :nanos 5400e9)))
    (is (= "30.0m" (util/format-duration :nanos 1800e9)))
    (is (= "1.50s" (util/format-duration :nanos 1500e6)))
    (is (= "500ms" (util/format-duration :nanos 500e6)))
    (is (= "250us" (util/format-duration :nanos 250e3)))
    (is (= "100ns" (util/format-duration :nanos 100))))

  (testing "format-duration with different input units"
    (is (= "1.0h" (util/format-duration :hours 1)))
    (is (= "1.0m" (util/format-duration :minutes 1)))
    (is (= "1.00s" (util/format-duration :seconds 1)))
    (is (= "1.00s" (util/format-duration :millis 1000)))
    (is (= "1ms" (util/format-duration :micros 1000))))

  (testing "format-duration with nil returns nil"
    (is (nil? (util/format-duration :millis nil)))))

(deftest title-case-test
  (testing "title-case conversion"
    (is (= "Simple Query" (util/title-case "simple-query")))
    (is (= "Multiple Word Query" (util/title-case "multiple-word-query")))
    (is (= "Single" (util/title-case "single")))
    (is (= "" (util/title-case "")))))

(deftest tpch-stage->query-row-test
  (testing "valid hot query stage"
    (let [stage {:stage "hot-queries-q1-pricing-summary"
                 :time-taken-ms 1500}
          result (summary/tpch-stage->query-row 0 stage)]
      (is (= "Hot" (:temp result)))
      (is (= "Q1" (:q result)))
      (is (= "Pricing Summary" (:query-name result)))
      (is (= 1500 (:time-taken-ms result)))))

  (testing "valid cold query stage"
    (let [stage {:stage "cold-queries-q5-local-supplier"
                 :time-taken-ms 2000}
          result (summary/tpch-stage->query-row 1 stage)]
      (is (= "Cold" (:temp result)))
      (is (= "Q5" (:q result)))
      (is (= "Local Supplier" (:query-name result)))))

  (testing "invalid stage returns nil"
    (is (nil? (summary/tpch-stage->query-row 0 {:stage "ingest" :time-taken-ms 1000})))))

(deftest tpch-summary->query-rows-test
  (testing "calculating query rows with percentages"
    (let [summary {:query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                  {:stage "cold-queries-q2-min-cost" :time-taken-ms 3000}]}
          result (summary/tpch-summary->query-rows summary)
          rows (:rows result)
          total (:total-ms result)]
      (is (= 4000 total))
      (is (= 2 (count rows)))
      (is (= "25.00%" (:percent-of-total (first rows))))
      (is (= "75.00%" (:percent-of-total (second rows))))))

  (testing "empty query stages"
    (let [summary {:query-stages []}
          result (summary/tpch-summary->query-rows summary)]
      (is (= 0 (:total-ms result)))
      (is (empty? (:rows result))))))

(deftest normalize-format-test
  (testing "valid formats"
    (is (= :table (summary/normalize-format :table)))
    (is (= :slack (summary/normalize-format "slack")))
    (is (= :github (summary/normalize-format "GITHUB")))
    (is (= :table (summary/normalize-format "Table"))))

  (testing "invalid formats default to :table"
    (is (= :table (summary/normalize-format :invalid)))
    (is (= :table (summary/normalize-format "xyz")))
    (is (= :table (summary/normalize-format nil)))))

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
                         "{\"stage\":\"summary\",\"time-taken-ms\":10000}"]))
        (let [result (log-parsing/parse-log "tpch" (.getPath temp-file))]
          (is (= 5 (count (:all-stages result))))
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
                         "{\"stage\":\"summary\",\"time-taken-ms\":5000}"]))
        (let [result (log-parsing/parse-log "yakbench" (.getPath temp-file))]
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
        (let [result (log-parsing/load-summary "tpch" (.getPath temp-file))]
          (is (= "tpch" (:benchmark-type result)))
          (is (contains? result :query-stages)))
        (finally
          (.delete temp-file)))))

  (testing "unsupported benchmark type throws error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unsupported benchmark type"
                          (log-parsing/load-summary "invalid" "dummy.log")))))

(deftest summary->table-tpch-test
  (testing "tpch table format"
    (let [test-summary {:benchmark-type "tpch"
                        :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                       {:stage "cold-queries-q2-min-cost" :time-taken-ms 2000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->table test-summary)]
      (is (string? result))
      (is (str/includes? result "Hot"))
      (is (str/includes? result "Q1"))
      (is (str/includes? result "Pricing"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->table-yakbench-test
  (testing "yakbench table format"
    (let [test-summary {:benchmark-type "yakbench"
                        :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                               :p90 1100000 :p99 1200000 :n 100 :sum 1000000}]}
                        :benchmark-total-time-ms 5000}
          result (summary/summary->table test-summary)]
      (is (string? result))
      (is (str/includes? result "profile1/q1"))
      (is (str/includes? result "percent-of-total"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty profiles still formats (validation happens earlier)"
    (let [test-summary {:benchmark-type "yakbench" :profiles {}}
          result (summary/summary->table test-summary)]
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->slack-tpch-test
  (testing "tpch slack format wrapped in code blocks"
    (let [test-summary {:benchmark-type "tpch"
                        :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->slack test-summary)]
      (is (str/includes? result "```"))
      (is (str/ends-with? result "```"))
      (is (str/includes? result "Hot Q1 Pricing"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty query stages still formats (validation happens earlier)"
    (let [test-summary {:benchmark-type "tpch" :query-stages []}
          result (summary/summary->slack test-summary)]
      (is (str/includes? result "```"))
      (is (str/includes? result "Total query time")))))

(deftest summary->slack-yakbench-test
  (testing "yakbench slack format with single query (no split)"
    (let [test-summary {:benchmark-type "yakbench"
                        :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000 :p99 1200000 :n 100 :sum 1000000}]}
                        :benchmark-total-time-ms 5000}
          result (summary/summary->slack test-summary)]
      (is (str/includes? result "```"))
      (is (str/ends-with? result "```"))
      (is (str/includes? result "profile1/q1"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))
      (is (not (str/includes? result "---SLACK-SPLIT---")))))

  (testing "yakbench slack format splits with multiple queries"
    (let [test-summary {:benchmark-type "yakbench"
                        :profiles {:p1 [{:id "q1" :mean 1000000 :p50 900000 :p99 1200000 :n 100 :sum 1000000}
                                        {:id "q2" :mean 2000000 :p50 1800000 :p99 2200000 :n 50 :sum 2000000}
                                        {:id "q3" :mean 3000000 :p50 2700000 :p99 3300000 :n 30 :sum 3000000}
                                        {:id "q4" :mean 4000000 :p50 3600000 :p99 4400000 :n 20 :sum 4000000}]}
                        :benchmark-total-time-ms 10000}
          result (summary/summary->slack test-summary)
          [first-part second-part] (str/split result #"---SLACK-SPLIT---")]
      (is (str/includes? result "---SLACK-SPLIT---"))
      (is (str/includes? first-part "Total query time"))
      (is (str/includes? first-part "```"))
      (is (str/includes? second-part "```")))))

(deftest summary->github-markdown-tpch-test
  (testing "tpch github markdown format"
    (let [test-summary {:benchmark-type "tpch"
                        :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}
                                       {:stage "cold-queries-q2-min-cost" :time-taken-ms 2000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->github-markdown test-summary)]
      (is (str/includes? result "| Temp | Query |"))
      (is (str/includes? result "|"))
      (is (str/includes? result "| Hot | Q1 |"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty query stages still formats (validation happens earlier)"
    (let [test-summary {:benchmark-type "tpch" :query-stages []}
          result (summary/summary->github-markdown test-summary)]
      (is (str/includes? result "Total query time")))))

(deftest summary->github-markdown-yakbench-test
  (testing "yakbench github markdown format"
    (let [test-summary {:benchmark-type "yakbench"
                        :profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                               :p90 1100000 :p99 1200000 :n 100 :sum 1000000}]}
                        :benchmark-total-time-ms 5000}
          result (summary/summary->github-markdown test-summary)]
      (is (str/includes? result "| Query | N | P50 |"))
      (is (str/includes? result "% of total"))
      (is (str/includes? result "| profile1/q1 |"))
      (is (str/includes? result "Total benchmark time"))))

  (testing "empty profiles still formats (validation happens earlier)"
    (let [test-summary {:benchmark-type "yakbench" :profiles {}}
          result (summary/summary->github-markdown test-summary)]
      (is (str/includes? result "Total query time")))))

(deftest yakbench-summary->query-rows-percent-test
  (testing "yakbench percent-of-total calculation"
    (let [test-summary {:profiles {:profile1 [{:id "q1" :mean 1000000 :p50 900000
                                               :p90 1100000 :p99 1200000 :n 100 :sum 1000000}
                                              {:id "q2" :mean 3000000 :p50 2700000
                                               :p90 3300000 :p99 3600000 :n 50 :sum 3000000}]}}
          result (summary/yakbench-summary->query-rows test-summary)
          rows (:rows result)
          total-ms (:total-ms result)]
      (is (= 2 (count rows)))
      (is (= "25.00%" (:percent-of-total (first rows))))
      (is (= "75.00%" (:percent-of-total (second rows))))
      (is (= 4.0 total-ms)))))

(deftest render-summary-test
  (testing "rendering with different formats"
    (let [test-summary {:benchmark-type "tpch"
                        :query-stages [{:stage "hot-queries-q1-pricing" :time-taken-ms 1000}]
                        :benchmark-total-time-ms 5000}]
      (is (string? (summary/render-summary test-summary {:format :table})))
      (is (str/includes? (summary/render-summary test-summary {:format :slack}) "```"))
      (is (str/includes? (summary/render-summary test-summary {:format :github}) "|"))))

  (testing "default format is table"
    (let [test-summary {:benchmark-type "tpch" :query-stages []}
          result (summary/render-summary test-summary {:format nil})]
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
          (.delete temp-file)))))

  (testing "summarize-log with empty readings query stages throws error"
    (let [temp-file (java.io.File/createTempFile "test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"ingest\",\"time-taken-ms\":1000}")
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"No query stages found"
                              (tasks/summarize-log ["readings" (.getPath temp-file)])))
        (finally
          (.delete temp-file))))))

(deftest parse-readings-log-test
  (testing "parsing a valid readings log"
    (let [temp-file (java.io.File/createTempFile "readings-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"stage\":\"ingest\",\"time-taken-ms\":100}"
                         "{\"stage\":\"sync\",\"time-taken-ms\":50}"
                         "{\"stage\":\"compact\",\"time-taken-ms\":25}"
                         "{\"stage\":\"query-recent-interval-day\",\"time-taken-ms\":1500}"
                         "{\"stage\":\"query-recent-interval-week\",\"time-taken-ms\":2000}"
                         "{\"stage\":\"query-offset-1-year-interval-day\",\"time-taken-ms\":1800}"
                         "{\"stage\":\"summary\",\"time-taken-ms\":10000}"]))
        (let [result (log-parsing/parse-log "readings" (.getPath temp-file))]
          (is (= 7 (count (:all-stages result))))
          (is (= 3 (count (:query-stages result))))
          (is (= 2 (count (:ingest-stages result))))
          (is (= 10000 (:benchmark-total-time-ms result)))
          (is (= "query-recent-interval-day" (name (:stage (first (:query-stages result)))))))
        (finally
          (.delete temp-file))))))

(deftest readings-stage->query-row-test
  (testing "recent interval query stage"
    (let [stage {:stage :query-recent-interval-day :time-taken-ms 1500}
          result (summary/readings-stage->query-row 0 stage)]
      (is (= "Recent Day" (:query result)))
      (is (= 1500 (:time-taken-ms result)))
      (is (= "query-recent-interval-day" (:stage result)))))

  (testing "offset query stage"
    (let [stage {:stage :query-offset-1-year-interval-week :time-taken-ms 2000}
          result (summary/readings-stage->query-row 1 stage)]
      (is (= "Offset 1 Year Week" (:query result)))
      (is (= 2000 (:time-taken-ms result))))))

(deftest readings-summary->query-rows-test
  (testing "calculating query rows with percentages"
    (let [test-summary {:query-stages [{:stage :query-recent-interval-day :time-taken-ms 1000}
                                       {:stage :query-recent-interval-week :time-taken-ms 3000}]}
          result (summary/readings-summary->query-rows test-summary)
          rows (:rows result)
          total (:total-ms result)]
      (is (= 4000 total))
      (is (= 2 (count rows)))
      (is (= "25.00%" (:percent-of-total (first rows))))
      (is (= "75.00%" (:percent-of-total (second rows))))))

  (testing "empty query stages"
    (let [test-summary {:query-stages []}
          result (summary/readings-summary->query-rows test-summary)]
      (is (= 0 (:total-ms result)))
      (is (empty? (:rows result))))))

(deftest summary->table-readings-test
  (testing "readings table format"
    (let [test-summary {:benchmark-type "readings"
                        :query-stages [{:stage :query-recent-interval-day :time-taken-ms 1000}
                                       {:stage :query-offset-1-year-interval-month :time-taken-ms 2000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->table test-summary)]
      (is (string? result))
      (is (str/includes? result "Recent Day"))
      (is (str/includes? result "Offset 1 Year Month"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->slack-readings-test
  (testing "readings slack format wrapped in code blocks"
    (let [test-summary {:benchmark-type "readings"
                        :query-stages [{:stage :query-recent-interval-day :time-taken-ms 1000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->slack test-summary)]
      (is (str/includes? result "```"))
      (is (str/ends-with? result "```"))
      (is (str/includes? result "Recent Day"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest summary->github-markdown-readings-test
  (testing "readings github markdown format"
    (let [test-summary {:benchmark-type "readings"
                        :query-stages [{:stage :query-recent-interval-day :time-taken-ms 1000}
                                       {:stage :query-recent-interval-week :time-taken-ms 2000}]
                        :benchmark-total-time-ms 5000}
          result (summary/summary->github-markdown test-summary)]
      (is (str/includes? result "| Query | Time (ms) |"))
      (is (str/includes? result "|"))
      (is (str/includes? result "| Recent Day |"))
      (is (str/includes? result "Total query time"))
      (is (str/includes? result "Total benchmark time")))))

(deftest parse-auctionmark-log-test
  (testing "parsing a valid auctionmark log"
    (let [temp-file (java.io.File/createTempFile "auctionmark-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"auctionmark\":{\"throughput\":1234.5,\"latency-p99\":50}}"
                         "{\"stage\":\"summary\",\"time-taken-ms\":60000}"]))
        (let [result (log-parsing/parse-log "auctionmark" (.getPath temp-file))]
          (is (= 1234.5 (get-in result [:auctionmark :auctionmark :throughput])))
          (is (= 60000 (:benchmark-total-time-ms result))))
        (finally
          (.delete temp-file)))))

  (testing "parsing auctionmark log without auctionmark data"
    (let [temp-file (java.io.File/createTempFile "auctionmark-test" ".log")]
      (try
        (spit temp-file "{\"stage\":\"summary\",\"time-taken-ms\":30000}")
        (let [result (log-parsing/parse-log "auctionmark" (.getPath temp-file))]
          (is (nil? (:auctionmark result)))
          (is (= 30000 (:benchmark-total-time-ms result))))
        (finally
          (.delete temp-file))))))

(deftest summary->table-auctionmark-test
  (testing "auctionmark table format with benchmark time"
    (let [test-summary {:benchmark-type "auctionmark"
                        :benchmark-total-time-ms 120000}
          result (summary/summary->table test-summary)]
      (is (string? result))
      (is (str/includes? result "Total benchmark time"))
      (is (str/includes? result "2.0m"))))

  (testing "auctionmark table format with nil benchmark time"
    (let [test-summary {:benchmark-type "auctionmark"
                        :benchmark-total-time-ms nil}
          result (summary/summary->table test-summary)]
      (is (str/includes? result "N/A")))))

(deftest summary->slack-auctionmark-test
  (testing "auctionmark slack format"
    (let [test-summary {:benchmark-type "auctionmark"
                        :benchmark-total-time-ms 120000}
          result (summary/summary->slack test-summary)]
      (is (string? result))
      (is (str/includes? result "Total benchmark time"))
      (is (str/includes? result "2.0m")))))

(deftest summary->github-markdown-auctionmark-test
  (testing "auctionmark github markdown format"
    (let [test-summary {:benchmark-type "auctionmark"
                        :benchmark-total-time-ms 120000}
          result (summary/summary->github-markdown test-summary)]
      (is (string? result))
      (is (str/includes? result "Total benchmark time"))
      (is (str/includes? result "2.0m")))))

(deftest summarize-log-auctionmark-test
  (testing "summarize-log for auctionmark does not require auctionmark data"
    (let [temp-file (java.io.File/createTempFile "auctionmark-test" ".log")]
      (try
        ;; auctionmark doesn't validate for empty data like other benchmarks
        (spit temp-file "{\"stage\":\"summary\",\"time-taken-ms\":60000}")
        (let [result (tasks/summarize-log ["auctionmark" (.getPath temp-file)])]
          (is (string? result))
          (is (str/includes? result "Total benchmark time")))
        (finally
          (.delete temp-file)))))

  (testing "summarize-log for auctionmark with full data"
    (let [temp-file (java.io.File/createTempFile "auctionmark-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"auctionmark\":{\"throughput\":500.0}}"
                         "{\"stage\":\"summary\",\"time-taken-ms\":90000}"]))
        (let [result (tasks/summarize-log ["auctionmark" (.getPath temp-file)])]
          (is (str/includes? result "1.5m")))
        (finally
          (.delete temp-file))))))

(deftest github-table-separator-test
  (testing "separator line has at least 3 dashes per column"
    (let [columns [{:key :name :header "Name"}
                   {:key :age :header "Age"}
                   {:key :score :header "Score"}]
          rows [{:name "Alice" :age 30 :score 95.5}
                {:name "Bob" :age 25 :score 87.3}]
          result (util/github-table columns rows)
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
          result (util/github-table columns rows)
          lines (str/split-lines result)
          separator-line (second lines)
          separator-parts (filter #(not (str/blank? %)) (str/split separator-line #"\|"))]
      (doseq [part separator-parts]
        (is (>= (count (filter #(= % \-) part)) 3)
            (format "Even short headers should have at least 3 dashes in separator: '%s'" part)))))

  (testing "separator line matches header width for long headers"
    (let [columns [{:key :long :header "Very Long Column Header Name"}]
          rows [{:long "value"}]
          result (util/github-table columns rows)
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

(deftest parse-clickbench-log-excludes-parent-ingest-test
  (testing "clickbench log parsing excludes parent ingest stage to avoid double counting"
    (let [temp-file (java.io.File/createTempFile "clickbench-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"stage\":\"submit-docs\",\"time-taken-ms\":5000}"
                         "{\"stage\":\"sync\",\"time-taken-ms\":3000}"
                         "{\"stage\":\"finish-block\",\"time-taken-ms\":1000}"
                         "{\"stage\":\"compact\",\"time-taken-ms\":2000}"
                         "{\"stage\":\"ingest\",\"time-taken-ms\":11000}"
                         "{\"stage\":\"summary\",\"time-taken-ms\":12000}"]))
        (let [result (log-parsing/parse-log "clickbench" (.getPath temp-file))
              ingest-stages (:ingest-stages result)
              stage-names (set (map :stage ingest-stages))]
          ;; parent "ingest" stage should NOT be in ingest-stages
          (is (not (contains? stage-names "ingest")))
          (is (= 4 (count ingest-stages)))
          ;; sum of sub-stages should be 11000, not 22000
          (is (= 11000 (reduce + (map :time-taken-ms ingest-stages)))))
        (finally
          (.delete temp-file))))))

(deftest parse-tsbs-iot-log-excludes-parent-ingest-test
  (testing "tsbs-iot log parsing excludes parent ingest stage to avoid double counting"
    (let [temp-file (java.io.File/createTempFile "tsbs-iot-test" ".log")]
      (try
        (spit temp-file
              (str/join "\n"
                        ["{\"stage\":\"gen+submit-docs\",\"time-taken-ms\":8000}"
                         "{\"stage\":\"sync\",\"time-taken-ms\":2000}"
                         "{\"stage\":\"finish-block\",\"time-taken-ms\":500}"
                         "{\"stage\":\"compact\",\"time-taken-ms\":1500}"
                         "{\"stage\":\"ingest\",\"time-taken-ms\":12000}"
                         "{\"stage\":\"queries\",\"time-taken-ms\":100}"
                         "{\"stage\":\"summary\",\"time-taken-ms\":12100}"]))
        (let [result (log-parsing/parse-log "tsbs-iot" (.getPath temp-file))
              ingest-stages (:ingest-stages result)
              stage-names (set (map :stage ingest-stages))]
          (is (not (contains? stage-names "ingest")))
          (is (= 4 (count ingest-stages)))
          (is (= 12000 (reduce + (map :time-taken-ms ingest-stages)))))
        (finally
          (.delete temp-file))))))

(deftest kubectl-command-construction-test
  (testing "kubectl passes args as separate strings to proc/shell"
    (let [captured-args (atom nil)]
      (with-redefs [proc/shell (fn [opts & args]
                                 (reset! captured-args args)
                                 {:exit 0 :out "{\"items\":[]}" :err ""})]
        (k8s/kubectl "get" "pods" "-n" "test-ns" "-l" "app=foo")
        (is (= ["kubectl" "get" "pods" "-n" "test-ns" "-l" "app=foo" "-o" "json"]
               (vec @captured-args))
            "Args should be separate strings, not joined"))))

  (testing "kubectl-get-pods constructs correct command with selector"
    (let [captured-args (atom nil)]
      (with-redefs [proc/shell (fn [opts & args]
                                 (reset! captured-args args)
                                 {:exit 0 :out "{\"items\":[]}" :err ""})]
        (k8s/kubectl-get-pods "cloud-benchmark" "app.kubernetes.io/component=benchmark")
        (is (= ["kubectl" "get" "pods" "-n" "cloud-benchmark" "-l" "app.kubernetes.io/component=benchmark" "-o" "json"]
               (vec @captured-args))))))

  (testing "kubectl-raw does not add -o json"
    (let [captured-args (atom nil)]
      (with-redefs [proc/shell (fn [opts & args]
                                 (reset! captured-args args)
                                 {:exit 0 :out "some output" :err ""})]
        (k8s/kubectl-raw "logs" "pod-name" "-n" "test-ns")
        (is (= ["kubectl" "logs" "pod-name" "-n" "test-ns"]
               (vec @captured-args))
            "kubectl-raw should not add -o json")))))

(deftest classify-job-test
  (testing "running job with active pods"
    (let [job {:metadata {:name "tpch"}
               :status {:active 1 :succeeded 0 :failed 0 :conditions []}}
          result (#'k8s/classify-job job)]
      (is (= "tpch" (:name result)))
      (is (:running? result))
      (is (not (:failed? result)))
      (is (not (:succeeded? result)))
      (is (not (:terminal? result)))))

  (testing "succeeded job"
    (let [job {:metadata {:name "tpch"}
               :status {:active 0 :succeeded 1 :failed 0
                        :conditions [{:type "Complete" :status "True"}]}}
          result (#'k8s/classify-job job)]
      (is (:succeeded? result))
      (is (:terminal? result))
      (is (not (:running? result)))))

  (testing "failed job"
    (let [job {:metadata {:name "tpch"}
               :status {:active 0 :succeeded 0 :failed 1
                        :conditions [{:type "Failed" :status "True"}]}}
          result (#'k8s/classify-job job)]
      (is (:failed? result))
      (is (:terminal? result))
      (is (not (:running? result)))))

  (testing "pending job (no pods yet)"
    (let [job {:metadata {:name "tpch"}
               :status {:active 0 :succeeded 0 :failed 0 :conditions []}}
          result (#'k8s/classify-job job)]
      (is (:pending? result))
      (is (not (:running? result)))
      (is (not (:terminal? result))))))

(deftest inspect-deployment-test
  (testing "running deployment"
    (with-redefs [k8s/kubectl-get-jobs (constantly {:items [{:metadata {:name "tpch"}
                                                             :status {:active 1 :succeeded 0 :failed 0 :conditions []}}]})]
      (let [result (k8s/inspect-deployment "cloud-benchmark")]
        (is (= "in_progress" (:status result)))
        (is (= ["tpch"] (:jobNames result)))
        (is (empty? (:failedJobs result))))))

  (testing "completed deployment"
    (with-redefs [k8s/kubectl-get-jobs (constantly {:items [{:metadata {:name "tpch"}
                                                             :status {:active 0 :succeeded 1 :failed 0
                                                                      :conditions [{:type "Complete" :status "True"}]}}]})]
      (let [result (k8s/inspect-deployment "cloud-benchmark")]
        (is (= "completed" (:status result)))
        (is (= ["tpch"] (:succeededJobs result))))))

  (testing "failed deployment"
    (with-redefs [k8s/kubectl-get-jobs (constantly {:items [{:metadata {:name "tpch"}
                                                             :status {:active 0 :succeeded 0 :failed 1
                                                                      :conditions [{:type "Failed" :status "True"}]}}]})]
      (let [result (k8s/inspect-deployment "cloud-benchmark")]
        (is (= "failed" (:status result)))
        (is (= ["tpch"] (:failedJobs result))))))

  (testing "empty namespace"
    (with-redefs [k8s/kubectl-get-jobs (constantly {:items []})]
      (let [result (k8s/inspect-deployment "cloud-benchmark")]
        (is (= "completed" (:status result)))
        (is (empty? (:jobNames result)))))))

(deftest container-failing-test
  (testing "terminated with non-zero exit"
    (let [status {:state {:terminated {:exitCode 1 :reason "Error"}}}]
      (is (#'k8s/container-failing? status))))

  (testing "terminated with zero exit is not failing"
    (let [status {:state {:terminated {:exitCode 0}}}]
      (is (not (#'k8s/container-failing? status)))))

  (testing "waiting with CrashLoopBackOff"
    (let [status {:state {:waiting {:reason "CrashLoopBackOff"}}}]
      (is (#'k8s/container-failing? status))))

  (testing "waiting with ImagePullBackOff"
    (let [status {:state {:waiting {:reason "ImagePullBackOff"}}}]
      (is (#'k8s/container-failing? status))))

  (testing "waiting with ErrImagePull"
    (let [status {:state {:waiting {:reason "ErrImagePull"}}}]
      (is (#'k8s/container-failing? status))))

  (testing "waiting with ContainerCreating is not failing"
    (let [status {:state {:waiting {:reason "ContainerCreating"}}}]
      (is (not (#'k8s/container-failing? status)))))

  (testing "running container is not failing"
    (let [status {:state {:running {:startedAt "2024-01-01T00:00:00Z"}}}]
      (is (not (#'k8s/container-failing? status))))))

(deftest pod-failing-test
  (testing "pod in Failed phase"
    (let [pod {:status {:phase "Failed" :containerStatuses []}}]
      (is (#'k8s/pod-failing? pod))))

  (testing "pod with crashing container"
    (let [pod {:status {:phase "Running"
                        :containerStatuses [{:state {:waiting {:reason "CrashLoopBackOff"}}}]}}]
      (is (#'k8s/pod-failing? pod))))

  (testing "pod with crashing init container"
    (let [pod {:status {:phase "Running"
                        :initContainerStatuses [{:state {:terminated {:exitCode 1}}}]
                        :containerStatuses [{:state {:waiting {:reason "PodInitializing"}}}]}}]
      (is (#'k8s/pod-failing? pod))))

  (testing "healthy running pod"
    (let [pod {:status {:phase "Running"
                        :containerStatuses [{:state {:running {:startedAt "2024-01-01"}}}]}}]
      (is (not (#'k8s/pod-failing? pod))))))

(deftest pod-running-stable-test
  (testing "stable running pod with completed init containers"
    (let [pod {:status {:phase "Running"
                        :initContainerStatuses [{:state {:terminated {:exitCode 0}}}]
                        :containerStatuses [{:state {:running {:startedAt "2024-01-01"}}}]}}]
      (is (#'k8s/pod-running-stable? pod))))

  (testing "running but init not done"
    (let [pod {:status {:phase "Running"
                        :initContainerStatuses [{:state {:running {}}}]
                        :containerStatuses [{:state {:waiting {:reason "PodInitializing"}}}]}}]
      (is (not (#'k8s/pod-running-stable? pod)))))

  (testing "pod not in Running phase"
    (let [pod {:status {:phase "Pending"
                        :containerStatuses []}}]
      (is (not (#'k8s/pod-running-stable? pod)))))

  (testing "running pod with no containers started"
    (let [pod {:status {:phase "Running"
                        :containerStatuses [{:state {:waiting {:reason "ContainerCreating"}}}]}}]
      (is (not (#'k8s/pod-running-stable? pod))))))

(deftest extract-params-edn-test
  (testing "extracts tpch params"
    (let [log "12:00:00.000 INFO xtdb.bench.tpch - {:scale-factor 1.0 :threads 4}"
          result (#'k8s/extract-params-edn log)]
      (is (= "{:scale-factor 1.0 :threads 4}" result))))

  (testing "extracts readings params"
    (let [log "12:00:00.000 INFO xtdb.bench.readings - {:scale-factor 0.1 :device-count 100}"
          result (#'k8s/extract-params-edn log)]
      (is (= "{:scale-factor 0.1 :device-count 100}" result))))

  (testing "returns nil when no match"
    (let [log "some random log line without params"]
      (is (nil? (#'k8s/extract-params-edn log))))))

(deftest extract-scale-factor-test
  (testing "extracts integer scale factor"
    (is (= 1.0 (#'k8s/extract-scale-factor "{:scale-factor 1}"))))

  (testing "extracts decimal scale factor"
    (is (= 0.01 (#'k8s/extract-scale-factor "{:scale-factor 0.01}"))))

  (testing "returns nil for non-matching string"
    (is (nil? (#'k8s/extract-scale-factor "{:threads 4}"))))

  (testing "returns nil for nil input"
    (is (nil? (#'k8s/extract-scale-factor nil)))))

(deftest extract-time-taken-ms-test
  (testing "extracts last time-taken-ms"
    (let [log "{\"stage\":\"q1\",\"time-taken-ms\":1000}\n{\"stage\":\"summary\",\"time-taken-ms\":5000}"]
      (is (= 5000 (#'k8s/extract-time-taken-ms log)))))

  (testing "handles single occurrence"
    (let [log "{\"stage\":\"summary\",\"time-taken-ms\":12345}"]
      (is (= 12345 (#'k8s/extract-time-taken-ms log)))))

  (testing "returns nil when not found"
    (is (nil? (#'k8s/extract-time-taken-ms "no time here")))))

(deftest compute-grafana-time-range-test
  (testing "returns default when file doesn't exist"
    (let [result (k8s/compute-grafana-time-range "/nonexistent/file.log")]
      (is (= {:from "now-2h" :to "now"} result))))

  (testing "returns default when file has no timestamp"
    (let [temp-file (java.io.File/createTempFile "grafana-test" ".log")]
      (try
        (spit temp-file "some log without timestamp")
        (let [result (k8s/compute-grafana-time-range (.getPath temp-file))]
          (is (= {:from "now-2h" :to "now"} result)))
        (finally
          (.delete temp-file)))))

  (testing "extracts timestamp and returns epoch millis"
    (let [temp-file (java.io.File/createTempFile "grafana-test" ".log")]
      (try
        (spit temp-file "12:30:45.123 INFO starting benchmark")
        (let [result (k8s/compute-grafana-time-range (.getPath temp-file))]
          ;; Should return epoch millis, not the string defaults
          (is (number? (:from result)))
          (is (number? (:to result)))
          (is (< (:from result) (:to result))))
        (finally
          (.delete temp-file))))))

(deftest wait-for-benchmark-completion-test
  (testing "job completes via job conditions"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions [{:type "Complete" :status "True"}]}})
                  k8s/kubectl-get-pods (constantly {:items []})]
      (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 1})]
        (is (= "success" (:status result)))
        (is (false? (:timedOut result))))))

  (testing "job fails via job conditions"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions [{:type "Failed" :status "True"}]}})
                  k8s/kubectl-get-pods (constantly {:items []})]
      (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 1})]
        (is (= "failure" (:status result)))
        (is (false? (:timedOut result))))))

  (testing "detects completion from pods when job has no conditions - job-name selector works"
    (let [call-count (atom 0)]
      (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions []}})
                    k8s/kubectl-get-pods (fn [_ns selector]
                                           (swap! call-count inc)
                                           (if (str/includes? selector "job-name=")
                                             {:items [{:status {:phase "Succeeded"}}]}
                                             {:items []}))]
        (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 2})]
          (is (= "success" (:status result)))
          (is (false? (:timedOut result)))))))

  (testing "falls back to component selector when job-name selector returns empty"
    (let [selectors-tried (atom [])]
      (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions []}})
                    k8s/kubectl-get-pods (fn [_ns selector]
                                           (swap! selectors-tried conj selector)
                                           (if (str/includes? selector "app.kubernetes.io/component")
                                             {:items [{:status {:phase "Succeeded"}}]}
                                             {:items []}))]
        (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 2})]
          (is (= "success" (:status result)))
          (is (some #(str/includes? % "job-name=") @selectors-tried)
              "Should have tried job-name selector")
          (is (some #(str/includes? % "app.kubernetes.io/component") @selectors-tried)
              "Should have fallen back to component selector")))))

  (testing "detects failure from pod phase"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions []}})
                  k8s/kubectl-get-pods (constantly {:items [{:status {:phase "Failed"}}]})]
      (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 2})]
        (is (= "failure" (:status result))))))

  (testing "times out when pods never become terminal"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions []}})
                  k8s/kubectl-get-pods (constantly {:items [{:status {:phase "Running"}}]})]
      (let [result (k8s/wait-for-benchmark-completion "tpch" {:sleep-seconds 0 :max-iterations 2})]
        (is (= "unknown" (:status result)))
        (is (true? (:timedOut result)))))))

(deftest derive-benchmark-status-test
  (testing "job completed successfully"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions [{:type "Complete" :status "True"}]}})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "success"} result)))))

  (testing "job failed"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions [{:type "Failed" :status "True"}]}})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "failure"} result)))))

  (testing "job still running"
    (with-redefs [k8s/kubectl (fn [& _] {:status {:conditions []}})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "unknown"} result)))))

  (testing "job not found, infer from pods - failed pod"
    (with-redefs [k8s/kubectl (constantly nil)
                  k8s/kubectl-get-pods (constantly {:items [{:status {:phase "Failed"}}]})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "failure"} result)))))

  (testing "job not found, infer from pods - succeeded pod"
    (with-redefs [k8s/kubectl (constantly nil)
                  k8s/kubectl-get-pods (constantly {:items [{:status {:phase "Succeeded"}}]})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "success"} result)))))

  (testing "job not found, no pods"
    (with-redefs [k8s/kubectl (constantly nil)
                  k8s/kubectl-get-pods (constantly {:items []})]
      (let [result (k8s/derive-benchmark-status "tpch")]
        (is (= {:status "unknown"} result))))))
