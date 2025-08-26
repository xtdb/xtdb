(ns xtdb.kafka.connect.e2e-test
  (:require [clojure.test :refer :all]
            [clojure.set :refer [rename-keys]]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.e2e-fixture :as fixture :refer [*xtdb-conn* *xtdb-db*]]
            [xtdb.kafka.connect.test.util :refer [query-col-types ->avro-record]]))

(use-fixtures :once fixture/with-containers)
(comment ; For dev
  (fixture/run-permanently)
  (fixture/stop-permanently))

(use-fixtures :each fixture/with-xtdb-conn)

(deftest ^:manual ingest_record_with_in-band_connect-schema
  (with-open [_ (fixture/with-connector
                  {:topics "my_table"
                   :value.converter "org.apache.kafka.connect.json.JsonConverter"
                   :transforms "xtdbEncode"
                   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
    (fixture/send-record! "my_table" "my_id"
      (json/write-value-as-string
        {:schema {:type "struct",
                  :fields [{:field :_id, :type "string", :optional false}
                           {:field :my_string, :type "string", :optional false}
                           {:field :my_int16, :type "int16", :optional false}
                           {:field :my_timestamptz, :type "string", :optional false, :parameters {:xtdb.type "timestamptz"}}
                           {:field :_valid_from, :type "string", :optional false, :parameters {:xtdb.type "timestamptz"}}]}
         :payload {:_id "my_id"
                   :my_string "my_string_value"
                   :my_int16 42
                   :my_timestamptz "2020-01-01T00:00:00Z"
                   :_valid_from "2020-01-02T00:00:00Z"}}))

    (Thread/sleep 3000)

    (is (= (xt/q *xtdb-conn* "SELECT *, _valid_from FROM my_table FOR VALID_TIME ALL")
           [{:xt/id "my_id"
             :my-string "my_string_value"
             :my-int16 42
             :my-timestamptz #xt/zdt"2020-01-01T00:00Z"
             :xt/valid-from #xt/zdt"2020-01-02T00:00Z[UTC]"}]))

    (is (= (query-col-types *xtdb-conn* "my_table")
           {:_id :utf8
            :my_string :utf8
            :my_int16 :i16
            :my_timestamptz [:timestamp-tz :micro "Z"]})))) ; TODO: should be UTC?


;; User Guide Examples:

(defn concat-maps [& ms]
  (apply array-map (mapcat identity (apply concat ms))))

(defn print-json-for-docs [obj]
  (println (json/write-value-as-string obj (json/object-mapper {:pretty true}))))

(defn with-connector*
  "Replaces some fake connector configuration parameters with actual test parameters."
  [example-conf]
  (fixture/with-connector
    (merge example-conf
           (when (:connection.url example-conf)
             {:connection.url (str "jdbc:xtdb://host.testcontainers.internal:5439/" *xtdb-db*)})
           (when (:value.converter.schema.registry.url example-conf)
             {:value.converter.schema.registry.url (fixture/schema-registry-base-url-for-containers)}))))

(defn await-readings-result []
  (Thread/sleep 3000)
  (let [results (xt/q *xtdb-conn* "SELECT *, _valid_from FROM readings FOR VALID_TIME ALL")]
    (when (empty? results)
      (println (-> fixture/*containers* ::fixture/connect .getLogs)))
    (->> results
      (map #(rename-keys % {:xt/id :_id
                            :xt/valid-from :_valid_from})))))

(def basic-example-conf
  {:tasks.max "1"
   :topics "readings"
   :connector.class "xtdb.kafka.connect.XtdbSinkConnector",
   :connection.url "jdbc:xtdb://xtdb_host:5432/xtdb"})

(def simple-example-conf
  (concat-maps basic-example-conf
               {:value.converter "org.apache.kafka.connect.json.JsonConverter"
                :value.converter.schemas.enable "false"}))

(def simple-example-data
  {:_id 1,
   :_valid_from "2025-08-25T11:15:00Z"
   :metric "Humidity",
   :measurement 0.8})

(comment
  (print-json-for-docs simple-example-conf)
  (print-json-for-docs simple-example-data))

(deftest ^:manual simple-example
  (with-open [_ (with-connector* simple-example-conf)]
    (fixture/send-record! "readings" "1" (json/write-value-as-string simple-example-data))
    (await-readings-result)
    (is (= (query-col-types *xtdb-conn* "readings")
           {:_id :i64
            :metric :utf8
            :measurement :f64}))))


(def rename-example-conf
  {:transforms "timeToString, xtdbRenames"
   :transforms.timeToString.type "org.apache.kafka.connect.transforms.TimestampConverter$Value"
   :transforms.timeToString.field "t"
   :transforms.timeToString.target.type "Timestamp"
   :transforms.xtdbRenames.type "org.apache.kafka.connect.transforms.ReplaceField$Value"
   :transforms.xtdbRenames.renames "reading_id:_id, t:_valid_from"})

(def rename-example-data
  {:reading_id 1,
   :t 1756120500000
   :metric "Humidity",
   :measurement 0.8})

(comment
  (print-json-for-docs rename-example-conf)
  (print-json-for-docs rename-example-data))

(deftest ^:manual rename-example
  (with-open [_ (with-connector* (merge simple-example-conf
                                        rename-example-conf))]
    (fixture/send-record! "readings" "1" (json/write-value-as-string rename-example-data))
    (await-readings-result)
    (is (= (query-col-types *xtdb-conn* "readings")
           {:_id :i64
            :metric :utf8
            :measurement :f64}))))


(def json-schema-example-conf
  {:value.converter "io.confluent.connect.json.JsonSchemaConverter"
   :value.converter.schemas.enable "true"
   :value.converter.schema.registry.url "http://schema-registry:8081"
   :transforms "xtdbEncode"
   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})

(def json-schema-example-schema
  {:type "object"
   :properties {:_id {:type "integer"}
                :_valid_from {:type "string"}
                :metric {:type "string"}
                :measurement {:type "number"
                              :connect.type "float32"}
                :span {:type "string"
                       :connect.parameters {:xtdb.type "interval"}}}})

(def json-schema-example-data
  {:_id 1,
   :_valid_from "2025-08-25T11:15:00Z"
   :metric "Humidity",
   :measurement 0.8
   :span "PT30S"})

(comment
  (print-json-for-docs json-schema-example-conf)
  (print-json-for-docs json-schema-example-schema)
  (print-json-for-docs json-schema-example-data))

(deftest ^:manual json-schema-example
  (with-open [_ (with-connector* (merge basic-example-conf
                                        json-schema-example-conf))]
    (let [schema-id (fixture/register-schema! {:subject "readings-value"
                                               :schema-type :json
                                               :schema json-schema-example-schema})]
      (fixture/send-record! "readings" "1"
        (-> json-schema-example-data
            (update-keys name))
        {:value-serializer :json-schema
         :schema-id schema-id})
      (await-readings-result)
      (is (= (query-col-types *xtdb-conn* "readings")
             {:_id :i64
              :metric :utf8
              :measurement :f32
              :span [:interval :month-day-micro]})))))


(def avro-schema-example-conf
  {:value.converter "io.confluent.connect.avro.AvroConverter"
   :value.converter.schemas.enable "true"
   :value.converter.connect.meta.data "true"
   :value.converter.schema.registry.url "http://schema-registry:8081"
   :transforms "xtdbEncode"
   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})

(def avro-schema-example-schema
  {:type "record"
   :name "Reading"
   :fields [{:name "_id", :type "long"}
            {:name "_valid_from", :type "string"}
            {:name "metric", :type "string"}
            {:name "measurement", :type "float"}
            {:name "span", :type {:type "string"
                                  :connect.parameters {:xtdb.type "interval"}}}]})

(def avro-schema-example-data
  {:_id 1,
   :_valid_from "2025-08-25T11:15:00Z"
   :metric "Humidity",
   :measurement 0.8
   :span "PT30S"})

(comment
  (print-json-for-docs avro-schema-example-conf)
  (print-json-for-docs avro-schema-example-schema)
  (print-json-for-docs avro-schema-example-data))

(deftest ^:manual avro-schema-example
  (with-open [_ (with-connector* (merge basic-example-conf
                                        avro-schema-example-conf))]
    (let [schema-id (fixture/register-schema! {:subject "readings-value"
                                               :schema-type :avro
                                               :schema avro-schema-example-schema})]
      (fixture/send-record! "readings" "1"
        (->avro-record avro-schema-example-schema avro-schema-example-data)
        {:value-serializer :avro
         :schema-id schema-id})
      (await-readings-result)
      (is (= (query-col-types *xtdb-conn* "readings")
             {:_id :i64
              :metric :utf8
              :measurement :f32
              :span [:interval :month-day-micro]})))))


(def connect-schema-example-conf
  {:value.converter "org.apache.kafka.connect.json.JsonConverter"
   :transforms "xtdbEncode"
   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})

(def connect-schema-example-data
  {:schema {:type "struct",
            :fields [{:field "_id", :type "int64", :optional false},
                     {:field "_valid_from", :type "string", :optional false},
                     {:field "metric", :type "string", :optional false},
                     {:field "measurement" :type "float", :optional false},
                     {:field "span", :type "string", :parameters {:xtdb.type "interval"}, :optional false}]}
   :payload {:_id 1,
             :_valid_from "2025-08-25T11:15:00Z"
             :metric "Humidity",
             :measurement 0.8
             :span "PT30S"}})

(comment
  (print-json-for-docs connect-schema-example-conf)
  (print-json-for-docs connect-schema-example-data))

(deftest ^:manual connect-schema-example
  (with-open [_ (with-connector* (merge basic-example-conf
                                        connect-schema-example-conf))]
    (fixture/send-record! "readings" "1"
      (json/write-value-as-string connect-schema-example-data))
    (await-readings-result)
    (is (= (query-col-types *xtdb-conn* "readings")
           {:_id :i64
            :metric :utf8
            :measurement :f32
            :span [:interval :month-day-micro]}))))
