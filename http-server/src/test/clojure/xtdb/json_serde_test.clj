(ns xtdb.json-serde-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.error :as err]
            [xtdb.serde :as serde])
  (:import (java.time Instant)
           (java.util UUID)
           (xtdb JsonSerde)
           (xtdb.api.query Basis QueryOptions QueryRequest)
           (xtdb.api.tx TxOptions)))

(defn- encode [v]
  (JsonSerde/encode v))

(defn- roundtrip-json-ld [v]
  (-> v JsonSerde/encode JsonSerde/decode))

(deftest test-json-ld-roundtripping
  (let [v {"keyword" :foo/bar
           "set-key" #{:foo :baz}
           "instant" #time/instant "2023-12-06T09:31:27.570827956Z"
           "date" #time/date "2020-01-01"
           "date-time" #time/date-time "2020-01-01T12:34:56.789"
           "zoned-date-time" #time/zoned-date-time "2020-01-01T12:34:56.789Z"
           "time-zone" #time/zone "America/Los_Angeles"
           "duration" #time/duration "PT3H1M35.23S"
           "period" #time/period "P18Y"
           "uuid" (UUID/randomUUID)}]
    (t/is (= v
             (roundtrip-json-ld v))
          "testing json ld values"))

  (let [ex (err/illegal-arg :divison-by-zero {:foo "bar"})
        roundtripped-ex (roundtrip-json-ld ex)]

    (t/testing "testing exception encoding/decoding"
      (t/is (= (ex-message ex)
               (ex-message roundtripped-ex)))

      (t/is (= (ex-data ex)
               (ex-data roundtripped-ex))))))

(defn- roundtrip-query-request [v]
  (-> v (JsonSerde/encode QueryRequest) (JsonSerde/decode QueryRequest)))

(defn- decode-query-request [^String v]
  (JsonSerde/decode v QueryRequest))

(deftest deserialize-query-map-test
  (let [tx-key (serde/->TxKey 1 #time/instant "2023-12-06T09:31:27.570827956Z")
        v (QueryRequest. "SELECT _id FROM docs"
                         (-> (QueryOptions/queryOpts)
                             (.args {"id" :foo})
                             (.basis (Basis. tx-key Instant/EPOCH))
                             (.afterTx tx-key)
                             (.txTimeout #time/duration "PT3H")
                             (.defaultTz #time/zone "America/Los_Angeles")
                             (.explain true)
                             (.keyFn #xt/key-fn :kebab-case-keyword)
                             (.build)))]
    (t/is (= v (roundtrip-query-request v))))

  (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                          (-> {"explain" true} encode decode-query-request))
        "query map without query"))

(defn- decode-query-options [^String v]
  (JsonSerde/decode v QueryOptions))

(deftest deserialize-list-args-test
  (t/is (= (-> (QueryOptions/queryOpts) (.args [1 2]) (.build))
           (-> {"args" [1 2]} encode decode-query-options))))
