(ns xtdb.grpc-server.service
  (:require [xtdb.api :as xt]
            [io.pedestal.http :as http]
            [io.pedestal.http.body-params :as body-params]
            [ring.util.response :as ring-resp]

            [xtdb.grpc-server.controllers :as controllers]

            ;; -- PROTOC-GEN-CLOJURE --
            [protojure.pedestal.core :as protojure.pedestal]
            [protojure.pedestal.routes :as proutes]
            [com.grpc.xtdb.GrpcApi.server :as grpc-api])
  (:gen-class))

(def node (xt/start-node {}))

(defn ping-page
  [_request]
  (ring-resp/response "pong"))

;; -- PROTOC-GEN-CLOJURE --
;; Implement our "Greeter" service interface.  The compiler generates
;; a defprotocol (greeter/Service, in this case), and it is our job
;; to define an implementation of every function within it.  These will be
;; invoked whenever a request arrives, similarly to if we had defined
;; these functions as pedestal defhandlers.  The main difference is that
;; the :body returned in the response should correlate to the protobuf
;; return-type declared in the Service definition within the .proto
;;
;; Note that our GRPC parameters are associated with the request-map
;; as :grpc-params, similar to how the pedestal body-param module
;; injects other types, like :json-params, :edn-params, etc.
;;
;; see http://pedestal.io/reference/request-map

(deftype XtdbGrpcAPI []
  grpc-api/Service
  (status [_this _request]
    {:status 200
     :body (controllers/status node)})
  (submit_tx [_this {{:keys [tx-ops]} :grpc-params :as _request}]
    (controllers/submit-tx node tx-ops)
    {:status 200
     :body {:tx_id 0}}))

;; Defines "/" and "/about" routes with their associated :get handlers.
;; The interceptors defined after the verb map (e.g., {:get home-page}
;; apply to / and its children (/about).
(def common-interceptors [(body-params/body-params) http/html-body])

;; Tabular routes
(def routes #{["/ping" :get (conj common-interceptors `ping-page)]})

;; -- PROTOC-GEN-CLOJURE --
;; Add the routes produced by Greeter->routes
(def grpc-routes (reduce conj routes (proutes/->tablesyntax {:rpc-metadata grpc-api/rpc-metadata :interceptors common-interceptors :callback-context (XtdbGrpcAPI.)})))
(def service {:env :prod
              :node (xt/start-node {})
              ::http/routes grpc-routes

              ;; -- PROTOC-GEN-CLOJURE --
              ;; We override the chain-provider with one provided by protojure.protobuf
              ;; and based on the Undertow webserver.  This provides the proper support
              ;; for HTTP/2 trailers, which GRPCs rely on.  A future version of pedestal
              ;; may provide this support, in which case we can go back to using
              ;; chain-providers from pedestal.
              ::http/type protojure.pedestal/config
              ::http/chain-provider protojure.pedestal/provider

              ;;::http/host "localhost"
              ::http/port 50051})
