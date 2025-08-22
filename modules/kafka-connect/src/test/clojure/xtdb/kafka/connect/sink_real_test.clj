(ns xtdb.kafka.connect.sink-real-test
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.fixture :as fixture :refer [*xtdb-conn*]]))

(use-fixtures :once fixture/with-containers)
(use-fixtures :each fixture/with-xtdb-conn)

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
                              :value-converter :json})
  (try
    (fixture/send-record! "my_table" "my_id"
      (json/write-value-as-string
        {:schema {:type "struct",
                  :fields [{:field "_id", :type "string", :optional false}
                           {:field "my_string", :type "string", :optional false}]}
         :payload {:_id "my_id"
                   :my_string "my_string_value"}}))

    (Thread/sleep 3000)

    (is (= (xt/q *xtdb-conn* "SELECT * FROM my_table FOR VALID_TIME ALL")
           [{:xt/id "my_id"
             :my-string "my_string_value"}]))

    (is (= (query-col-types) {:_id :utf8
                              :my_string :utf8

                              :_system_from [:timestamp-tz :micro "UTC"]
                              :_system_to [:union #{:null [:timestamp-tz :micro "UTC"]}]
                              :_valid_from [:timestamp-tz :micro "UTC"]
                              :_valid_to [:union #{:null [:timestamp-tz :micro "UTC"]}]}))
    (finally
      (fixture/delete-connector! "my_table"))))
