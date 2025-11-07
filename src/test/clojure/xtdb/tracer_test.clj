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
