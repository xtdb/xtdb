(ns crux.soak.main
  (:require [ring.util.response :as resp]
            [ring.middleware.params :as params ]
            [bidi.ring]
            [ring.adapter.jetty :as jetty]
            [crux.api :as api]
            [clojure.string :as string]
            [hiccup2.core :as h]))

(def server-id (java.util.UUID/randomUUID))

(def locations
  ["Milton-Keynes" "Macclesfield" "Market-Harborough" "Huddersfield" "Stratford-upon-Avon" "Whaley-Bridge" "Coventry" "Edinburgh" "Bournemouth" "Rhosneigr" "Cardiff" "London" "Southampton" "Leicester" "Lincoln" "York" "Nottingham" "Chesterfield" "Buxton" "Bristol"])

(def soak-secrets
  (-> (System/getenv "SOAK_SECRETS")
      (json/decode)))

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

(defn wrap-node [handler node]
  (fn [request]
    (handler (assoc request :crux-node node))))

(defn weather-handler [req]
  (let [location (get-in req [:query-params "Location"])
        node (:crux-node req)]
    (resp/response
     (str
      (h/html
       [:header
        [:link {:rel "stylesheet" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
       [:body
        [:h1 (str location)]
        [:p (str (api/entity (api/db node) (keyword (str (-> (string/lower-case location)) "-current"))))]])))))


(defn homepage-handler [req]
  (resp/response
   (str
    (h/html
     [:header
      [:link {:rel "stylesheet" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
     [:body
      [:h1 "Crux Weather Service"]
      [:h2 "Locations"]
      [:form {:target "_blank" :action "/weather.html"}
       [:p
        (into [:select {:name "Location"}]
              (map
               (fn [location] [':option {':value location} location])
               locations))]
       [:p [:input {:type "submit" :value "View Location"}]]]]))))

(def bidi-handler
  (bidi.ring/make-handler ["" [["/" {"index.html" homepage-handler
                                     "weather.html" weather-handler}]
                               [true (bidi.ring/->Redirect 307 homepage-handler)]]]))

(defn -main [& args]
  (let [node (api/start-node {:crux.node/topology '[crux.kafka/topology crux.kv.rocksdb/kv-store]
                              :crux.kafka/bootstrap-servers (get soak-secrets "CONFLUENT_BROKER")
                              :crux.kafka/kafka-properties-map kafka-properties-map})]
    (jetty/run-jetty (fn [req]
                       ((-> bidi-handler
                            (params/wrap-params)
                            (wrap-node node)) req))
                     {:port 8080 :join? false})))
