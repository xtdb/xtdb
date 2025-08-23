(ns xtdb.kafka.connect.e2e-test
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.e2e-fixture :as fixture :refer [*xtdb-conn*]]))

(use-fixtures :once fixture/with-containers)
(use-fixtures :each fixture/with-xtdb-conn)

(comment ; For dev
  (fixture/run-permanently)
  (fixture/stop-permanently))

(defn query-col-types []
  (let [col-types-res (xt/q *xtdb-conn* "SELECT column_name, data_type
                                         FROM information_schema.columns
                                         WHERE table_name = 'my_table'")]
    (->> col-types-res
         (map (fn [{:keys [column-name data-type]}]
                [(keyword column-name) (read-string data-type)]))
         (into {}))))

(deftest ^:manual ingest_record_with_in-band_connect-schema
  (fixture/create-connector! {:topic "my_table"
                              :value-converter :json
                              :extra-conf {:id.mode "record_value"
                                           :id.field "_id"
                                           :validFrom.field "_valid_from"

                                           :transforms "xtdbEncode"
                                           :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"}})
  (try
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

    (is (= (query-col-types) {:_id :utf8
                              :my_string :utf8
                              :my_int16 :i16
                              :my_timestamptz [:timestamp-tz :micro "Z"] ; TODO: should be UTC?

                              :_system_from [:timestamp-tz :micro "UTC"]
                              :_system_to [:union #{:null [:timestamp-tz :micro "UTC"]}]
                              :_valid_from [:timestamp-tz :micro "UTC"]
                              :_valid_to [:union #{:null [:timestamp-tz :micro "UTC"]}]}))
    (finally
      (fixture/delete-connector! "my_table"))))
