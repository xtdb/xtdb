(ns xtdb.client.impl
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as hato]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.middleware :as hato.middleware]
            [juxt.clojars-mirrors.reitit-core.v0v5v15.reitit.core :as r]
            [xtdb.protocols :as xtp]
            [xtdb.error :as err]
            [xtdb.transit :as xt.transit]
            [xtdb.util :as util])
  (:import [java.io EOFException InputStream]
           java.lang.AutoCloseable
           java.util.concurrent.CompletableFuture
           java.util.function.Function
           java.util.NoSuchElementException
           xtdb.IResultSet))

(def transit-opts
  {:decode {:handlers xt.transit/tj-read-handlers}
   :encode {:handlers xt.transit/tj-write-handlers}})

(def router
  (r/router xtp/http-routes))

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
                         :version :http-1.1
                         :url (str (:base-url client)
                                   (-> (r/match-by-name router endpoint)
                                       (r/match->path)))
                         :transit-opts transit-opts
                         :async? true}
                        opts)
                 identity handle-err)))

(deftype TransitResultSet [^InputStream in, rdr
                           ^:unsynchronized-mutable next-el]
  IResultSet
  (hasNext [this]
    (or (some? next-el)
        (try
          (set! (.next-el this) (transit/read rdr))
          true
          (catch RuntimeException e
            (if (instance? EOFException (.getCause e))
              false
              (throw e))))))

  (next [this]
    (when-not (.hasNext this)
      (throw (NoSuchElementException.)))
    (let [el (.next-el this)]
      (set! (.next-el this) nil)
      (if (instance? Throwable el)
        (throw el)
        el)))

  (close [_]
    (.close in)))

(defmethod hato.middleware/coerce-response-body ::transit+json->result-or-error [_req {:keys [^InputStream body status] :as resp}]
  (letfn [(parse-body [rdr->body]
            (try
              (let [rdr (transit/reader body :json {:handlers xt.transit/tj-read-handlers})]
                (-> resp (assoc :body (rdr->body rdr))))
              (catch Exception e
                (.close body)
                (throw e))))]

    (if (hato.middleware/unexceptional-status? status)
      (parse-body (fn [rdr] (->TransitResultSet body rdr nil)))

      ;; This should be an error we know how to decode
      (parse-body transit/read))))

(defn validate-query-opts [{:keys [key-fn] :as _query-opts}]
  (some-> key-fn util/validate-remote-key-fn))

(defrecord XtdbClient [base-url, !latest-submitted-tx]
  xtp/PNode
  (open-query& [client query {:keys [basis] :as query-opts}]
    (validate-query-opts query-opts)
    (let [{basis-tx :tx} basis
          ^CompletableFuture !basis-tx (if (instance? CompletableFuture basis-tx)
                                         basis-tx
                                         (CompletableFuture/completedFuture basis-tx))]
      (-> !basis-tx
          (.thenCompose (reify Function
                          (apply [_ basis-tx]
                            (request client :post :query
                                     {:content-type :transit+json
                                      :form-params (-> query-opts
                                                       (assoc :query query)
                                                       (assoc-in [:basis :tx] basis-tx)
                                                       (update :basis xtp/after-latest-submitted-tx client))
                                      :as ::transit+json->result-or-error}))))
          (.thenApply (reify Function
                        (apply [_ resp]
                          (:body resp)))))))

  (latest-submitted-tx [_] @!latest-submitted-tx)

  xtp/PSubmitNode
  (submit-tx& [client tx-ops]
    (xtp/submit-tx& client tx-ops {}))

  (submit-tx& [client tx-ops opts]
    (-> ^CompletableFuture
        (request client :post :tx
                 {:content-type :transit+json
                  :form-params {:tx-ops tx-ops
                                :opts opts}})

        (.thenApply (reify Function
                      (apply [_ resp]
                        (let [tx (:body resp)]
                          (swap! !latest-submitted-tx xtp/max-tx tx)
                          tx))))))

  xtp/PStatus
  (status [client]
    (-> @(request client :get :status)
        :body))

  AutoCloseable
  (close [_]))

(defn start-client ^java.lang.AutoCloseable [url]
  (->XtdbClient url (atom nil)))
