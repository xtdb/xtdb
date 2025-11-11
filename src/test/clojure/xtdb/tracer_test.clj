(ns xtdb.tracer-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import (io.opentelemetry.api.common Attributes AttributeKey)
           (io.opentelemetry.sdk.common CompletableResultCode)
           (io.opentelemetry.sdk.trace.data SpanData)
           (io.opentelemetry.sdk.trace.export SimpleSpanProcessor SpanExporter)))

(defn- test-span-exporter [!spans]
  (reify SpanExporter
    (export [_ span-data]
            (swap! !spans concat span-data)
            (CompletableResultCode/ofSuccess))
    (flush [_] (CompletableResultCode/ofSuccess))
    (shutdown [_] (CompletableResultCode/ofSuccess))))

(t/deftest test-query-creates-span
  (t/testing "running a query creates a pgwire.query span"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})] 
        
        (xt/q node "SELECT 1")

        ;; Give spans a moment to be exported
        (Thread/sleep 100)

        (let [current-spans @!spans
              ^SpanData span (first current-spans)
              ^Attributes span-attributes (.getAttributes span)
              ^AttributeKey attribute-key (AttributeKey/stringKey "db.statement") ]
          (t/is (= 1 (count current-spans)))
          (t/is (= "pgwire.query" (.getName span))) 
          (t/is (= "SELECT 1" (.get span-attributes attribute-key))))))))

(t/deftest test-query-creates-cursor-spans
  (t/testing "running a query creates individual spans for each cursor"
    (let [!spans (atom [])
          exporter (test-span-exporter !spans)
          span-processor (SimpleSpanProcessor/create exporter)]
      (with-open [node (xtn/start-node
                        {:tracer {:enabled? true
                                  :service-name "xtdb-test"
                                  :span-processor span-processor}})]

        ;; Create a table and insert some data to generate more complex query plan
        (xt/submit-tx node [[:put-docs :test_table {:xt/id 1 :value "a"}]
                            [:put-docs :test_table {:xt/id 2 :value "b"}]])

        ;; Run a query that will use multiple cursors (scan and select)
        (xt/q node "SELECT * FROM test_table WHERE value = 'a'")

        ;; Give spans a moment to be exported
        (Thread/sleep 200)

        (let [current-spans @!spans
              span-names (mapv #(.getName ^SpanData %) current-spans)
              cursor-spans (filterv #(.startsWith ^String % "query.cursor.") span-names)]

          ;; Should have the pgwire.query span plus cursor spans for scan and select
          (t/is (>= (count current-spans) 2) "Should have at least pgwire.query and cursor spans")
          (t/is (some #(= "pgwire.query" %) span-names) "Should have pgwire.query span")
          (t/is (seq cursor-spans) "Should have cursor spans")
          
          ;; Verify we have spans for different cursor types
          (t/is (some #(= "query.cursor.scan" %) cursor-spans) "Should have scan cursor span")
          (t/is (some #(= "query.cursor.select" %) cursor-spans) "Should have select cursor span")

          ;; Verify cursor spans have proper attributes with timing information
          (doseq [^SpanData span current-spans
                  :when (.startsWith (.getName span) "query.cursor.")]
            (let [^Attributes attrs (.getAttributes span)
                  cursor-type-key (AttributeKey/stringKey "cursor.type")
                  total-time-key (AttributeKey/stringKey "cursor.total_time_ms")]
              (t/is (.get attrs cursor-type-key) "Cursor span should have cursor.type attribute")
              (t/is (.get attrs total-time-key) "Cursor span should have cursor.total_time_ms attribute"))))))))
