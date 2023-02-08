(ns xtdb.healthcheck
  "Healthcheck for XTDB."
  (:require
   [clojure.pprint :as pp]
   [xtdb.system :as sys]
   [xtdb.bus :as bus]
   [xtdb.api :as xt]
   [xtdb.tx :as tx]
   [xtdb.node :as node]
   [xtdb.checkpoint :as ckpt]
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

(defn- ->routes
  [events]
  (let [->event-type (fn [m] (get m ::xt/event-type))]
    (ccore/routes
     (ccore/GET "/healthz" params (str (->> @events (map ->event-type) last)))
     (croute/not-found "Page not found!"))))

(defn ->server {::sys/deps {:bus :xtdb/bus}
                ::sys/before [[:xtdb/index-store :kv-store]]
                ::sys/args {:port {:spec :xtdb.io/port
                                   :doc "Port to start the healthcheck HTTP server on"
                                   :default default-server-port}
                            :jetty-opts
                            {:doc "Extra options to pass to Jetty, see https://ring-clojure.github.io/ring/ring.adapter.jetty.html"}}}
  [{:keys [bus port jetty-opts] :as options}]
  (assert bus)
  (let [events (atom [{::xt/event-type ::node/node-starting}])
        app (->routes events)
        ^Server server (jetty/run-jetty app (merge {:port port :join? false} jetty-opts))
        listener (bus/listen bus {::xt/event-types
                                  #{::node/node-closing
                                    ::ckpt/index-restoring
                                    ::tx/tx-ingester-starting}}
                             #(swap! events conj %))]
    (->HTTPServer server listener events options)))
