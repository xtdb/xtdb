(ns crux.soak.ingest
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as string]
            [crux.api :as crux])
  (:import java.time.Duration
           java.util.Date))

(def locations
  {"Macclesfield" "2643266",
   "Huddersfield" "2646458",
   "Milton-Keynes" "2642465",
   "Stratford-upon-Avon" "2636713",
   "Coventry" "2652221",
   "Edinburgh" "2650225",
   "Bournemouth" "2655095",
   "Rhosneigr" "2639435",
   "Market-Harborough" "2643027",
   "Cardiff" "2653822",
   "London" "2643743",
   "Southampton" "2637487",
   "Leicester" "2644668",
   "Lincoln" "2644487",
   "York" "2633352",
   "Nottingham" "2641170",
   "Chesterfield" "2653225",
   "Buxton" "2654141",
   "Whaley-Bridge" "2634194",
   "Bristol" "2654675"})

(def soak-secrets
  (-> (System/getenv "SOAK_SECRETS")
      (json/decode)))

(defn fetch-current-weather [location-id]
  (-> (http/get "http://api.openweathermap.org/data/2.5/weather"
                {:query-params {"id" location-id, "appid" (get soak-secrets "WEATHER_API_TOKEN")}
                 :accept :json
                 :as :json})
      :body))

(defn fetch-weather-forecast [location-id]
  (-> (http/get "http://api.openweathermap.org/data/2.5/forecast"
                {:query-params {"id" location-id, "appid" (get soak-secrets "WEATHER_API_TOKEN")}
                 :accept :json
                 :as :json})
      :body))

(defn parse-dt [dt]
  (Date. ^long (* 1000 dt)))

(defn ->put [doc ^Date valid-time ^Duration valid-duration]
  [:crux.tx/put doc valid-time (Date/from (-> (.toInstant valid-time)
                                              (.plus valid-duration)))])

(defn current-resp->put [resp]
  (let [location-name (:name resp)]
    (->put (-> resp
               (assoc :crux.db/id (-> (string/lower-case location-name)
                                      (string/replace #" " "-")
                                      (str "-current")
                                      (keyword))
                      :location-name location-name)
               (select-keys [:crux.db/id :location-name :main :wind :weather]))
           (parse-dt (:dt resp))
           (Duration/ofHours 1))))

(defn forecast-resp->puts [resp]
  (let [location-name (get-in resp [:city :name])]
    (for [forecast (:list resp)]
      (->put (-> forecast
                 (assoc :crux.db/id (-> (string/lower-case location-name)
                                        (string/replace #" " "-")
                                        (str "-forecast")
                                        (keyword))
                        :location-name location-name)
                 (select-keys [:crux.db/id :location-name :main :wind :weather]))
             (parse-dt (:dt forecast))
             (Duration/ofHours 3)))))

(def kafka-properties-map
  {"ssl.endpoint.identification.algorithm" "https"
   "sasl.mechanism" "PLAIN"
   "sasl.jaas.config" (str (->> ["org.apache.kafka.common.security.plain.PlainLoginModule"
                                 "required"
                                 (format "username=\"%s\"" (get soak-secrets "CONFLUENT_API_TOKEN"))
                                 (format "password=\"%s\"" (get soak-secrets "CONFLUENT_API_SECRET"))]
                                (string/join " "))
                           ";")
   "security.protocol" "SASL_SSL"})

(defn -main [& args]
  (let [mode (first args)]
    (with-open [node (crux/new-ingest-client {:crux.kafka/bootstrap-servers (get soak-secrets "CONFLUENT_BROKER")
                                              :crux.kafka/replication-factor 3
                                              :crux.kafka/tx-topic "soak-transaction-log"
                                              :crux.kafka/doc-topic "soak-docs"
                                              :crux.kafka/doc-partitions 1
                                              :crux.kafka/kafka-properties-map kafka-properties-map})]
      (crux/submit-tx node
                      (case mode
                        "current" (->> (vals locations)
                                       (map fetch-current-weather)
                                       (map current-resp->put))

                        "forecast" (->> (vals locations)
                                        (map fetch-weather-forecast)
                                        (mapcat forecast-resp->puts))))))

  (shutdown-agents))
