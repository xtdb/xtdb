(ns crux.console
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [crux-ui-server.main :as ui]
            [crux-ui-server.pages :as pages]
            [crux.console.crux-handler :as crux]
            [crux.node :as n]
            [crux.io :as cio]
            [ring.adapter.jetty :as j]
            [ring.util.request :as req]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.params :as p]
            [crux.api :as api])
  (:import java.io.Closeable ))

(def module
  (merge n/base-topology
         {::console-server
          {:start-fn (fn [{:keys [crux.node/node]} {::keys [console-port routes-prefix]}]
                       (let [crux-handler (crux/wrapped-handler node)
                             combined-handler (fn [req]
                                                (let [path (req/path-info req)]
                                                  (if (re-find #"\A\/crux" path)
                                                    (crux-handler req)
                                                    (ui/handler req))))]
                         (reset! ui/routes (ui/calc-routes routes-prefix))
                         (reset! ui/config
                                 {:console/frontend-port console-port
                                  :console/routes-prefix routes-prefix
                                  :console/crux-node-url-base (str "localhost:" console-port "/crux")
                                  :console/hide-features #{:features/attribute-history}
                                  :console/crux-http-port console-port})
                         (reset! pages/routes-prefix routes-prefix)
                         (log/info "Crux Console started on port: " console-port)
                         (http/start-server combined-handler {:port console-port})))
           :deps #{:crux.node/node}
           :args {::console-port {:crux.config/type :crux.config/nat-int
                                  :doc "Port to start the console frontend on"
                                  :default 5000}
                  ::routes-prefix {:crux.config/type :crux.config/string
                                   :doc "Console route prefix"
                                   :default "/console"}}}}))
