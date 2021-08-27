(ns ^:no-doc xtdb.soak.ingest
  (:require [clj-http.client :as http]
            [clojure.string :as string]
            [xtdb.api :as xt]
            [xtdb.kafka :as k]
            [xtdb.soak.config :as config]
            [nomad.config :as n])
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

(defn fetch-current-weather [location-id]
  (-> (http/get "http://api.openweathermap.org/data/2.5/weather"
                {:query-params {"id" location-id, "appid" config/weather-api-token}
                 :accept :json
                 :as :json})
      :body))

(defn fetch-weather-forecast [location-id]
  (-> (http/get "http://api.openweathermap.org/data/2.5/forecast"
                {:query-params {"id" location-id, "appid" config/weather-api-token}
                 :accept :json
                 :as :json})
      :body))

(defn parse-dt [dt]
  (Date. ^long (* 1000 dt)))

(defn ->put [doc ^Date valid-time ^Duration valid-duration]
  [:xt/put doc valid-time (Date/from (-> (.toInstant valid-time)
                                         (.plus valid-duration)))])

(defn current-resp->put [resp]
  (let [location-name (:name resp)]
    (->put (-> resp
               (assoc :xt/id (-> (string/lower-case location-name)
                                 (string/replace #" " "-")
                                 (str "-current")
                                 (keyword))
                      :location-name location-name)
               (select-keys [:xt/id :location-name :main :wind :weather]))
           (parse-dt (:dt resp))
           (Duration/ofHours 1))))

(defn forecast-resp->puts [resp]
  (let [location-name (get-in resp [:city :name])]
    (for [forecast (:list resp)]
      (->put (-> forecast
                 (assoc :xt/id (-> (string/lower-case location-name)
                                   (string/replace #" " "-")
                                   (str "-forecast")
                                   (keyword))
                        :location-name location-name)
                 (select-keys [:xt/id :location-name :main :wind :weather]))
             (parse-dt (:dt forecast))
             (Duration/ofHours 3)))))

(defn -main [& args]
  (n/set-defaults! {:secret-keys {:soak (config/load-secret-key)}})

  (let [mode (first args)]
    (with-open [node (xt/new-submit-client [{:xt/tx-log {:xt/module `k/->tx-log}
                                             :xt/document-store {:xt/module `k/->submit-only-document-store}}
                                            (config/xt-node-config)])]
      (xt/submit-tx node
                    (case mode
                      "current" (->> (vals locations)
                                     (map fetch-current-weather)
                                     (map current-resp->put))

                      "forecast" (->> (vals locations)
                                      (map fetch-weather-forecast)
                                      (mapcat forecast-resp->puts))))))

  (shutdown-agents))
