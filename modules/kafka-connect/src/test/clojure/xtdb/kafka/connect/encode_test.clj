(ns xtdb.kafka.connect.encode-test
  (:require [clojure.test :refer :all]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.encode :as encode]
            [xtdb.node :as xtn]
            [xtdb.kafka.connect.test.util :refer [->sink-record ->struct query-col-types]])
  (:import (clojure.lang ExceptionInfo)
           (java.util ArrayList)
           (org.apache.kafka.connect.data ConnectSchema Schema SchemaBuilder)))

(deftest encode-by-schema_called_with_Struct_succeeds
  (let [^ConnectSchema schema (-> (SchemaBuilder/struct)
                                  (.field "_id" Schema/STRING_SCHEMA)
                                  (.field "my_int64" Schema/INT64_SCHEMA)
                                  (.field "my_int32" Schema/INT32_SCHEMA)
                                  (.field "my_int16" Schema/INT16_SCHEMA)
                                  (.field "my_int8" Schema/INT8_SCHEMA)

                                  (.field "my_float64" Schema/FLOAT64_SCHEMA)
                                  (.field "my_float32" Schema/FLOAT32_SCHEMA)

                                  (.field "my_timestamp" (-> (SchemaBuilder/string)
                                                             (.parameter "xtdb.type" "timestamptz")
                                                             (.build)))
                                  (.field "my_interval" (-> (SchemaBuilder/string)
                                                            (.parameter "xtdb.type" "interval")
                                                            (.build)))

                                  (.field "my_int32_array" (-> (SchemaBuilder/array Schema/INT32_SCHEMA)
                                                               (.build)))
                                  (.field "my_map_of_int32" (-> (SchemaBuilder/map Schema/STRING_SCHEMA Schema/INT32_SCHEMA)
                                                                (.build)))
                                  (.build))

        value (->struct schema {:_id "my_id"
                                :my_int64 42
                                :my_int32 (int 42)
                                :my_int16 (short 42)
                                :my_int8 (byte 42)
                                :my_float64 1.0
                                :my_float32 (float 1.0)
                                :my_timestamp "2020-01-01T00:00:00Z"
                                :my_interval "P1DT1H"
                                :my_int32_array (ArrayList. [(int 1) (int 2) (int 3)])
                                :my_map_of_int32 {"key1" (int 1)
                                                  "key2" (int 2)}})

        record (->sink-record {:topic "my_topic"
                               :key-value "_id_value"
                               :value-schema schema
                               :value-value value})

        encoded-record (encode/encode-record-value-by-schema record)]
    (with-open [node (xtn/start-node)]
      (xt/execute-tx node [[:put-docs :my-table (-> encoded-record
                                                    .value
                                                    (update-keys keyword)
                                                    (clojure.set/rename-keys {:_id :xt/id}))]])
      (is (= (query-col-types node "my_table")
             {:_id :utf8

              :my_int64 :i64
              :my_int32 :i32
              :my_int16 :i16
              :my_int8 :i8

              :my_float64 :f64
              :my_float32 :f32

              :my_timestamp [:timestamp-tz :micro "Z"]     ; TODO: should be UTC ?!
              :my_interval [:interval :month-day-micro]

              :my_int32_array [:list :i32]
              :my_map_of_int32 [:struct {'key1 :i32, 'key2 :i32}]})))))

(deftest encode-by-schema_throws_errors_with_path
  (let [^ConnectSchema subschema (-> (SchemaBuilder/struct)
                                     (.field "my_int64" Schema/INT64_SCHEMA)
                                     (.build))
        ^ConnectSchema schema (-> (SchemaBuilder/struct)
                                  (.field "my_struct" subschema)
                                  (.build))

        exc (with-redefs [encode/?encode-by-simple-type (fn [& _]
                                                          (throw (Exception. "boo!")))]
              (is (thrown? ExceptionInfo (encode/encode-by-schema schema (->struct schema
                                                                           {:my_struct (->struct subschema
                                                                                         {:my_int64 42})})))))
        _ (is (= (-> exc ex-data ::encode/path) ["my_struct" "my_int64"]))

        exc (is (thrown? ExceptionInfo (encode/encode-by-schema schema "invalid")))
        _ (is (= (-> exc ex-data ::encode/path) []))]))
