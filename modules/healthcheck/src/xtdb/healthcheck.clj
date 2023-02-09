(ns xtdb.healthcheck
  "Healthcheck for XTDB."
  (:require
   [clojure.data.json :as json]
   [xtdb.system :as sys]
   [xtdb.bus :as bus]
   [xtdb.api :as xt]
   [ring.adapter.jetty :as jetty]
   [compojure.core :as ccore]
   [compojure.route :as croute])
  (:import
   [java.io Closeable]
   org.eclipse.jetty.server.Server))

(defrecord HTTPServer [^Server server listener events options]
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
  (let [->event-type (fn [m] (dissoc m ::xt/event-type))]
    (ccore/routes
     (ccore/context "/healthz" []
                    (ccore/GET "/hist" []
                               (json/write-str (->> @events
                                                    #_(sort-by :timestamp)
                                                    (mapv ->event-type)))))
     (croute/not-found "Page not found!"))))

(defn ->server {::sys/deps {:bus :xtdb/bus}
                ::sys/before [[:xtdb/index-store :kv-store]]
                ::sys/args {:port {:spec :xtdb.io/port
                                   :doc "Port to start the healthcheck HTTP server on"
                                   :default default-server-port}
                            :jetty-opts
                            {:doc "Extra options to pass to Jetty, see https://ring-clojure.github.io/ring/ring.adapter.jetty.html"}}}
  [{:keys [bus port jetty-opts] :as options}]
  (let [events (atom [(update (bus/->event :healthz :xtdb.node/node-starting) :clock adjust-clock)])
        app (->routes events)
        ^Server server (jetty/run-jetty app (merge {:port port :join? false} jetty-opts))
        listener (bus/listen bus {::xt/event-types #{:xtdb.node/node-closing :healthz}}
                             #(swap! events conj (update % :clock adjust-clock)))]
    (->HTTPServer server listener events options)))
