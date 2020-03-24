(ns crux.soak.main
  (:require bidi.ring
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.soak.config :as config]
            [hiccup2.core :refer [html]]
            [integrant.core :as ig]
            [integrant.repl :as ir]
            [nomad.config :as n]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [ring.util.response :as resp])
  (:import crux.api.NodeOutOfSyncException
           java.io.Closeable
           java.text.SimpleDateFormat
           (java.time Duration Instant)
           java.util.Date
           org.eclipse.jetty.server.Server))

(defn wrap-node [handler node]
  (fn [request]
    (handler (assoc request :crux-node node))))

(defn get-current-weather [node location valid-time]
  (api/entity (api/db node valid-time) (keyword (str location "-current"))))

(defn get-weather-forecast [node location valid-time]
  (let [past-five-days (->> (iterate #(.minus ^Instant % (Duration/ofDays 1))
                                     (.toInstant valid-time))
                            (take 5)
                            (map #(Date/from %)))]
    (->> past-five-days
         (keep (fn [transaction-time]
                 (try
                   [transaction-time
                    (api/entity
                     (api/db node valid-time transaction-time)
                     (keyword (str location "-forecast")))]
                   (catch NodeOutOfSyncException e
                     nil)))))))

(def date-formatter (SimpleDateFormat. "dd/MM/yyyy"))

(defn filter-weather-map [{:keys [main wind weather]}]
  (let [filtered-main (-> (set/rename-keys main {:temp :temperature})
                          (select-keys [:temperature :feels_like :pressure :humidity]))]
    (when-not (empty? filtered-main)
      (-> filtered-main
          (assoc :wind_speed (:speed wind))
          (assoc :current_weather (:description (first weather)))))))

(defn render-weather-map [weather-map]
  (for [[k v] weather-map]
    [:p
     [:b (-> (str (name k) ": ")
             (string/replace #"_" " ")
             string/upper-case)]
     v]))

(defn render-current-weather [current-weather]
  [:div#current-weather
   [:h2 "Current Weather"]
   (into [:div#weather]
         (->> current-weather
              filter-weather-map
              render-weather-map))])

(defn render-weather-forecast [weather-forecast]
  [:div#weather-forecast
   [:h2 "Forecast History"]
   (into [:div#forecasts]
         (for [[forecast-date forecast] weather-forecast]
           (into [:div#forecast
                  [:h3 (.format date-formatter forecast-date)]]
                 (->> forecast
                      filter-weather-map
                      render-weather-map))))])

(defn render-weather-page [{:keys [location-id location-name date
                                   current-weather weather-forecast]}]
  (str
   (html
    [:head
     [:link {:rel "stylesheet" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
    [:body
     [:style "body { display: inline-flex; font-family: Helvetica }
                 #location { text-align: center; width: 300px; padding-right: 30px; }
                 #current-weather { width: 300px; padding-right: 30px; }
                 #forecasts { display: inline-flex; }
                 #forecast { padding-right: 10px; }
                 #forecast h3 { margin-top: 0px; } "]
     [:div#location
      [:h1 location-name]
      [:form {:action "/weather.html"}
       [:input {:type "hidden" :name "Location" :value location-id}]
       (into [:select {:name "Date"}]
             (for [next-date (->> (iterate #(.plus ^Instant % (Duration/ofDays 1)) (.toInstant (Date.)))
                                  (take 5)
                                  (map #(Date/from %)))]
               (let [formatted-date (.format date-formatter next-date)]
                 [:option {:value (.getTime next-date)
                           :selected (when (= formatted-date (.format date-formatter date)) "selected")}
                  formatted-date])))
       [:p [:input {:type "submit" :value "Select Date"}]]]]
     (when current-weather
       (render-current-weather current-weather))
     (render-weather-forecast weather-forecast)])))

(defn weather-handler [req]
  (let [location-id (get-in req [:query-params "Location"])
        date (some-> (get-in req [:query-params "Date"]) (Long/parseLong) (Date.))
        node (:crux-node req)]
    (resp/response
     (render-weather-page {:location-id location-id
                           :location-name (-> (api/entity (api/db node) (keyword (str location-id "-current")))
                                              (:location-name))
                           :date date
                           :current-weather (get-current-weather node location-id date)
                           :weather-forecast (get-weather-forecast node location-id date)}))))

(defn render-homepage [location-names]
  (str
   (html
    [:head
     [:link {:rel "stylesheet" :type "text/css" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
    [:body
     [:div#homepage
      [:style "#homepage { text-align: center; font-family: Helvetica; }
                h1 { font-size: 3em; }
                h2 { font-size: 2em; }"]
      [:h1 "Crux Weather Service"]
      [:h2 "Locations"]
      [:form {:target "_blank" :action "/weather.html"}
       [:input {:type "hidden" :name "Date" :value (System/currentTimeMillis)}]
       [:p
        (into [:select {:name "Location"}]
              (map
               (fn [location-name]
                 [:option {:value (-> (string/replace location-name #" " "-")
                                      (string/lower-case))}
                  location-name])
               location-names))]
       [:p [:input {:type "submit" :value "View Location"}]]]]])))

(defn homepage-handler [req]
  (let [node (:crux-node req)
        location-names (->> (api/q (api/db node) '{:find [l]
                                                   :where [[e :location-name l]]})
                            (map first))]
    (resp/response
     (render-homepage location-names))))

(def bidi-handler
  (bidi.ring/make-handler ["" [["/" {"index.html" homepage-handler
                                     "weather.html" weather-handler}]
                               [true (bidi.ring/->Redirect 307 homepage-handler)]]]))

(defmethod ig/init-key :soak/crux-node [_ node-opts]
  (let [node (api/start-node node-opts)]
    (log/info "Loading Weather Data...")
    (api/sync node)
    (log/info "Weather Data Loaded!")
    node))

(defmethod ig/halt-key! :soak/crux-node [_ ^Closeable node]
  (.close node))

(defmethod ig/init-key :soak/jetty-server [_ {:keys [crux-node server-opts]}]
  (log/info "Starting jetty server...")
  (jetty/run-jetty (-> bidi-handler
                       (params/wrap-params)
                       (wrap-node crux-node))
                   server-opts))

(defmethod ig/halt-key! :soak/jetty-server [_ ^Server server]
  (.stop server))

(defn config []
  {:soak/crux-node (merge {:crux.node/topology ['crux.kafka/topology 'crux.kv.rocksdb/kv-store]}
                          (config/crux-node-config))
   :soak/jetty-server {:crux-node (ig/ref :soak/crux-node)
                       :server-opts {:port 8080, :join? false}}})

(defn -main [& args]
  (n/set-defaults! {:secret-keys {:soak (config/load-secret-key)}})
  (ir/set-prep! config)
  (ir/go))
