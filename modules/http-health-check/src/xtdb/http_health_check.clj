(ns xtdb.http-health-check
  "Healthcheck for XTDB."
  (:require
   [clojure.data.json :as json]
   [clojure.string :as s]
   [xtdb.system :as sys]
   [xtdb.bus :as bus]
   [xtdb.api :as xt]
   [juxt.clojars-mirrors.ring-jetty-adapter.v0v14v2.ring.adapter.jetty9 :as j]
   [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.interceptor]
   [juxt.clojars-mirrors.reitit-ring.v0v5v15.reitit.ring :as rr])
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

(defn- process-event
  [{::xt/keys [event-type] :as event}]
  (if-not (:clock event)
    {::xt/event-type :internal
     :namespace (namespace event-type)
     :event (name event-type)
     :timestamp (java.util.Date.)
     :clock (adjust-clock (System/nanoTime))}
    (update event :clock adjust-clock)))

(defn- ->lag-event
  [lag]
  {::xt/event-type :internal
   :namespace "xtdb.tx-indexing"
   :event lag
   :timestamp (java.util.Date.)
   :clock (adjust-clock (System/nanoTime))})

(defn- tx-ingester-lag!
  [{:keys [events xtdb-node]}]
  (when xtdb-node
    (let [completed (xt/latest-completed-tx xtdb-node)
          submitted (xt/latest-submitted-tx xtdb-node)]
      (swap! events conj (->lag-event {:latest-completed-tx completed
                                       :latest-submitted-tx submitted})))))

(defn- ->xtdb-router
  [{:keys [events] :as opts}]
  (rr/router
   [["/healthz"
     ["/" (fn [_]
            (tx-ingester-lag! opts)
            (json/write-str
             (->> (group-by :namespace @events)
                  ((fn [m] (for [[k v] m] [k (last v)])))
                  (map second)
                  (sort-by :clock))))]
     ["/hist" (fn [_]
                (tx-ingester-lag! opts)
                (json/write-str @events))]
     ["/ns/:ns" (fn [{:keys [:path-params]}]
                  (tx-ingester-lag! opts)
                  (json/write-str
                   (filter #(s/includes? (:namespace %) (:ns path-params)) @events)))]]]))

(defprotocol EventSnapshot
  (get-snapshot [_]))

(defrecord XTDBEventSink [xtdb-node ^Closeable listener events]
  EventSnapshot
  (get-snapshot [_]
    (tx-ingester-lag! {:xtdb-node xtdb-node :events events})
    (->> (group-by :namespace @events)
         ((fn [m] (for [[k v] m] [k (last v)])))
         (map second)
         (sort-by :clock)))
  Closeable
  (close [_]
    (.close listener)))

(defn ->xtdb-event-sink
  [xtdb-node]
  (let [events (atom [(process-event {::xt/event-type :xtdb.node/node-starting})])
        ^Closeable listener (bus/listen (:bus xtdb-node)
                                        {::xt/event-types #{:xtdb.node/node-closing
                                                            :xtdb.node/slow-query
                                                            :healthz}}
                                        #(swap! events conj (process-event %)))]
    (->XTDBEventSink xtdb-node listener events)))

(defn ->server {::sys/deps {:bus :xtdb/bus}
                ::sys/before [[:xtdb/index-store :kv-store]]
                ::sys/args {:port {:spec :xtdb.io/port
                                   :doc "Port to start the health-check HTTP server on"
                                   :default default-server-port}
                            :jetty-opts {:doc "Extra options to pass to Jetty"}}}
  [{:keys [bus port jetty-opts] :as options}]
  (let [events (atom [(process-event {::xt/event-type :xtdb.node/node-starting})])
        ^Server server (j/run-jetty
                        (rr/ring-handler (->xtdb-router {:events events})
                                         (rr/routes
                                          (rr/create-resource-handler {:path "/"})
                                          (rr/create-default-handler)))
                        (merge {:port port :join? false} jetty-opts))
        listener (bus/listen bus {::xt/event-types #{:xtdb.node/node-closing
                                                     :xtdb.node/slow-query
                                                     :healthz}}
                             #(swap! events conj (process-event %)))]
    (->HTTPServer server listener events options)))
