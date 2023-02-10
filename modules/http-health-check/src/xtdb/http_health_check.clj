(ns xtdb.http-health-check
  "Healthcheck for XTDB."
  (:require
   [clojure.data.json :as json]
   [clojure.string :as s]
   [xtdb.system :as sys]
   [xtdb.bus :as bus]
   [xtdb.api :as xt]
   [ring.adapter.jetty :as jetty]
   [compojure.core :as ccore]
   [compojure.route :as croute])
  (:import
   [java.io Closeable]
   org.eclipse.jetty.server.Server))

(defrecord HTTPServer [^Server server ^Closeable listener events options]
  Closeable
  (close [_]
    (.stop server)
    (.close listener)))

(def ^:const default-server-port 7000)

(defonce base-time (System/nanoTime))

(defn- adjust-clock
  [now]
  (/ (- now base-time) (double 1e9)))

(defn- ->routes
  [events]
  (ccore/routes
   (ccore/context "/healthz" []
     (ccore/GET "/" [] (json/write-str
                        (->> (group-by :namespace @events)
                             ((fn [m] (for [[k v] m] [k (last v)])))
                             (map second)
                             (sort-by :clock))))
     (ccore/GET "/hist" [] (json/write-str @events))
     (ccore/GET "/ns/:ns" [ns] (json/write-str
                                (filter #(s/includes? (:namespace %) ns) @events))))
   (croute/not-found "Not found!")))

(defn ->server {::sys/deps {:bus :xtdb/bus}
                ::sys/before [[:xtdb/index-store :kv-store]]
                ::sys/args {:port {:spec :xtdb.io/port
                                   :doc "Port to start the health-check HTTP server on"
                                   :default default-server-port}
                            :jetty-opts {:doc "Extra options to pass to Jetty"}}}
  [{:keys [bus port jetty-opts] :as options}]
  (let [events (atom [(update (bus/->event :healthz :xtdb.node/node-starting) :clock adjust-clock)])
        app (->routes events)
        ^Server server (jetty/run-jetty app (merge {:port port :join? false} jetty-opts))
        listener (bus/listen bus {::xt/event-types #{:xtdb.node/node-closing :healthz}}
                             #(swap! events conj (update % :clock adjust-clock)))]
    (->HTTPServer server listener events options)))
