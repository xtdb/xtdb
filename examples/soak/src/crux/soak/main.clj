(ns ^:no-doc crux.soak.main
  (:require [bidi.bidi :as bidi]
            [bidi.ring :as br]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.api :as xt]
            [xtdb.rocksdb :as rocksdb]
            [crux.soak.config :as config]
            [crux.checkpoint :as cp]
            [xtdb.kafka :as k]
            [xtdb.s3.checkpoint :as s3.cp]
            [hiccup2.core :refer [html]]
            [integrant.core :as ig]
            [integrant.repl :as ir]
            [nomad.config :as n]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [ring.util.response :as resp])
  (:import crux.api.NodeOutOfSyncException
           java.io.Closeable
           [java.time Duration LocalDate LocalTime ZonedDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date
           org.eclipse.jetty.server.Server))

(def london (ZoneId/of "Europe/London"))

(def routes
  ["" [["/" {"index.html" :homepage
             "weather.html" :weather}]
       ["/resources" (br/->ResourcesMaybe {:prefix "public"})]
       ["/status" :status]
       [true (br/->Redirect 307 :homepage)]]])

(defn get-current-weather [node location valid-time]
  (let [db (xt/db node valid-time)
        eid (keyword (str location "-current"))]
    (when-let [etx (xt/entity-tx db eid)]
      (merge (xt/entity db eid)
             (select-keys etx [:xt/valid-time :xt/tx-time])))))

(defn get-weather-forecast [node location ^Date valid-time]
  (->> (for [tx-time (->> (iterate #(.minus ^ZonedDateTime % (Duration/ofDays 1))
                                   (-> (ZonedDateTime/ofInstant (.toInstant valid-time) london)
                                       (.with (LocalTime/of 1 0))))
                          (take 5)
                          (map #(Date/from (.toInstant %))))]
         (when-let [db (try
                         (xt/db node valid-time tx-time)
                         (catch NodeOutOfSyncException e
                           nil))]
           (let [eid (keyword (str location "-forecast"))]
             (when-let [etx (xt/entity-tx db eid)]
               (merge (xt/entity db eid)
                      (select-keys etx [:xt/valid-time :xt/tx-time]))))))
       (remove nil?)
       distinct))

(defn filter-weather-map [{:keys [main wind weather]}]
  (let [filtered-main (-> (set/rename-keys main {:temp :temperature})
                          (select-keys [:temperature :feels_like :pressure :humidity]))]
    (when-not (empty? filtered-main)
      (-> filtered-main
          (assoc :wind_speed (:speed wind))
          (assoc :current_weather (:description (first weather)))
          (update :temperature (comp #(- % 273) bigdec))
          (update :feels_like (comp #(- % 273) bigdec))))))

(defn render-time [^Date time]
  (.format (DateTimeFormatter/ofPattern "dd/MM/yyyy HH:mm")
           (ZonedDateTime/ofInstant (.toInstant time) london)))

(defn render-weather-report [weather-report]
  [:div.weather-reports
   [:div.bitemp-inst
    "vt = "
    (render-time (:xt/valid-time weather-report))]
   [:div.bitemp-inst
    "tt = "
    (render-time (:xt/tx-time weather-report))]
   (for [[k v] (filter-weather-map weather-report)]
     [:p.weather-report
      [:b (-> (str (name k) ": ")
              (string/replace #"_" " ")
              string/upper-case)]
      v])])

(defn render-weather-page [{:keys [location-id location-name ^Date valid-time
                                   current-weather weather-forecast]}]
  (str
   (html
    [:head
     [:link {:rel "stylesheet" :type "text/css" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]
     [:link {:rel "stylesheet" :type "text/css" :href "/resources/style.css"}]]
    [:body#weather-body
     [:div#location
      [:h1 location-name]
      (let [zdt (ZonedDateTime/ofInstant (.toInstant valid-time) london)]
        [:form {:action (bidi/path-for routes :weather)}
         [:input {:type "hidden" :name "location" :value location-id}]
         [:p [:input {:type "date",
                      :name "date",
                      :value (.format DateTimeFormatter/ISO_LOCAL_DATE zdt)}]]
         [:p [:input {:type "time",
                      :name "time",
                      :value (.format (DateTimeFormatter/ofPattern "HH:mm") zdt)}]]
         [:p [:input {:type "submit" :value "Select Date"}]]])]
     (when current-weather
       [:div#current-weather
        [:h2 "Current Weather"]
        (render-weather-report current-weather)])
     [:div#weather-forecast
      [:h2 "Forecast History"]
      [:div#forecasts
       (->> weather-forecast (map render-weather-report))]]])))

(defn weather-handler [req {:keys [crux-node]}]
  (let [location-id (get-in req [:query-params "location"])
        valid-time (-> (ZonedDateTime/of (or (some-> (get-in req [:query-params "date"])
                                                     (LocalDate/parse))
                                             (LocalDate/now london))
                                         (or (some-> (get-in req [:query-params "time"])
                                                     (LocalTime/parse))
                                             (LocalTime/now london))
                                         london)
                       .toInstant
                       (Date/from))]
    (resp/response
     (render-weather-page {:location-id location-id
                           :location-name (-> (xt/entity (xt/db crux-node)
                                                          (keyword (str location-id "-current")))
                                              (:location-name))
                           :valid-time valid-time
                           :current-weather (get-current-weather crux-node location-id valid-time)
                           :weather-forecast (get-weather-forecast crux-node location-id valid-time)}))))

(defn render-homepage [location-names]
  (str
   (html
    [:head
     [:link {:rel "stylesheet" :type "text/css" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]
     [:link {:rel "stylesheet" :type "text/css" :href "/resources/style.css"}]]
    [:body
     [:div#homepage
      [:h1 "Crux Weather Service"]
      [:h2 "Locations"]
      [:form {:target "_blank" :action "/weather.html"}
       [:p
        (into [:select {:name "location"}]
              (for [location-name (sort location-names)]
                [:option {:value (-> (string/replace location-name #" " "-")
                                     (string/lower-case))}
                 location-name]))]
       [:p [:input {:type "submit" :value "View Location"}]]]]])))

(defn homepage-handler [req {:keys [crux-node]}]
  (let [location-names (->> (xt/q (xt/db crux-node) '{:find [l]
                                                        :where [[e :location-name l]]})
                            (map first))]
    (resp/response (render-homepage location-names))))

(defn status-handler [req {:keys [crux-node]}]
  (let [status (xt/status crux-node)]
    {:status (if (or (not (contains? status :crux.zk/zk-active?))
                     (:crux.zk/zk-active? status))
               200
               500)
     :headers {"Content-Type" "application/edn"}
     :body (pr-str status)}))

(defn bidi-handler [{:keys [crux-node]}]
  (br/make-handler routes
                   (some-fn {:homepage #(homepage-handler % {:crux-node crux-node})
                             :weather #(weather-handler % {:crux-node crux-node})
                             :status #(status-handler % {:crux-node crux-node})}
                            identity
                            (constantly (constantly (resp/not-found "Not found"))))))

(defmethod ig/init-key :soak/crux-node [_ node-opts]
  (let [node (xt/start-node node-opts)]
    (log/info "Loading Weather Data...")
    (xt/sync node)
    (log/info "Weather Data Loaded!")
    node))

(defmethod ig/halt-key! :soak/crux-node [_ ^Closeable node]
  (.close node))

(defmethod ig/init-key :soak/jetty-server [_ {:keys [crux-node server-opts]}]
  (log/info "Starting jetty server...")
  (jetty/run-jetty (-> (bidi-handler {:crux-node crux-node})
                       (params/wrap-params))
                   server-opts))

(defmethod ig/halt-key! :soak/jetty-server [_ ^Server server]
  (.stop server))

(defn config []
  {:soak/crux-node [{:xt/tx-log {:xt/module `k/->tx-log}
                     :xt/document-store {:xt/module `k/->document-store
                                         :local-document-store {:kv-store ::rocksdb}}
                     ::rocksdb {:xt/module `rocksdb/->kv-store
                                :db-dir (io/file "indexes")
                                :checkpointer {:xt/module `cp/->checkpointer
                                               :store {:xt/module `s3.cp/->cp-store
                                                       :bucket "crux-soak-checkpoints"}
                                               :approx-frequency (Duration/ofHours 6)}}
                     :xt/index-store {:kv-store ::rocksdb}}
                    (config/crux-node-config)]
   :soak/jetty-server {:crux-node (ig/ref :soak/crux-node)
                       :server-opts {:port 8080, :join? false}}})

(defn -main [& args]
  (n/set-defaults! {:secret-keys {:soak (config/load-secret-key)}})
  (ir/set-prep! #'config)
  (ir/go))
