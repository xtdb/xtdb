(ns core2.client
  (:require [core2.api :as c2]
            [core2.transit :as c2.transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as hato]
            [reitit.core :as r]
            [core2.error :as err])
  (:import clojure.lang.IReduceInit
           java.lang.AutoCloseable
           java.util.concurrent.CompletableFuture
           java.util.function.Function))

(def transit-opts
  {:decode {:handlers c2.transit/tj-read-handlers}
   :encode {:handlers c2.transit/tj-write-handlers}})

(def router
  (r/router c2/http-routes))

(defn- handle-err [e]
  (throw (or (when-let [body (:body (ex-data e))]
               (when (::err/error-type (ex-data body))
                 body))
             e)))

(defn- request
  ([client request-method endpoint]
   (request client request-method endpoint {}))

  ([client request-method endpoint opts]
   (hato/request (merge {:accept :transit+json
                         :as :transit+json
                         :request-method request-method
                         :url (str (:base-url client)
                                   (-> (r/match-by-name router endpoint)
                                       (r/match->path)))
                         :transit-opts transit-opts
                         :async? true}
                        opts)
                 identity handle-err)))

(defrecord Core2Client [base-url]
  c2/PClient
  (plan-query-async [client query params]
    (let [basis-tx (get-in query [:basis :tx])
          ^CompletableFuture !basis-tx (if (instance? CompletableFuture basis-tx)
                                         basis-tx
                                         (CompletableFuture/completedFuture basis-tx))]
      (-> !basis-tx
          (.thenCompose (reify Function
                          (apply [_ basis-tx]
                            (request client :post :query
                                     {:content-type :transit+json
                                      :form-params {:query (-> query
                                                               (assoc-in [:basis :tx] basis-tx))
                                                    :params params}}))))
          (.thenApply (reify Function
                        (apply [_ resp]
                          (reify IReduceInit
                            (reduce [_ f init]
                              (reduce f init (:body resp))))))))))

  c2/PSubmitNode
  (submit-tx [client tx-ops]
    (-> ^CompletableFuture
        (request client :post :tx
                 {:content-type :transit+json
                  :form-params {:tx-ops tx-ops}})

        (.thenApply (reify Function
                      (apply [_ resp]
                        (:body resp))))))

  c2/PStatus
  (status [client]
    (-> @(request client :get :status)
        :body))

  AutoCloseable
  (close [_]))

(defn start-client ^core2.client.Core2Client [url]
  (->Core2Client url))
