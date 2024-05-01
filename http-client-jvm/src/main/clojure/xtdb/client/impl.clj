(ns xtdb.client.impl
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as hato]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.middleware :as hato.middleware]
            [juxt.clojars-mirrors.reitit-core.v0v5v15.reitit.core :as r]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import [java.io EOFException InputStream]
           java.lang.AutoCloseable
           [java.util.concurrent CompletableFuture ExecutionException]
           java.util.function.Function
           java.util.Spliterator
           [java.util.stream StreamSupport]
           xtdb.api.IXtdb
           (xtdb.api.query Basis QueryOptions XtqlQuery)))

(def transit-opts
  {:decode {:handlers serde/transit-read-handlers}
   :encode {:handlers serde/transit-write-handlers}})

(def router
  (r/router xtp/http-routes))

(defn- request
  ([client request-method endpoint]
   (request client request-method endpoint {}))

  ([client request-method endpoint opts]
   (try
     (hato/request (merge {:accept :transit+json
                           :as :transit+json
                           :request-method request-method
                           :version :http-1.1
                           :url (str (:base-url client)
                                     (-> (r/match-by-name router endpoint)
                                         (r/match->path)))
                           :transit-opts transit-opts}
                          opts))
     (catch Exception e
       (throw (or (when-let [body (:body (ex-data e))]
                    (when (ex-data body)
                      body))
                  e))))))

(deftype TransitSpliterator [rdr]
  Spliterator
  (tryAdvance [_ c]
    (try
      (let [el (transit/read rdr)]
        (if (instance? Throwable el)
          (throw el)
          (.accept c el)))
      true
      (catch RuntimeException e
        (if (instance? EOFException (.getCause e))
          false
          (throw e)))))

  (characteristics [_] Spliterator/IMMUTABLE)
  (trySplit [_] nil)
  (estimateSize [_] Long/MAX_VALUE))

(defmethod hato.middleware/coerce-response-body ::transit+json->result-or-error [_req {:keys [^InputStream body status] :as resp}]
  (try
    (let [rdr (transit/reader body :json {:handlers serde/transit-read-handlers})]
      (if (hato.middleware/unexceptional-status? status)
        (-> resp
            (assoc :body (StreamSupport/stream (->TransitSpliterator rdr) false)))

        ;; This should be an error we know how to decode
        (throw (transit/read rdr))))

    (catch Throwable t
      (.close body)
      (throw t))))

(defn- open-query [client query query-opts]
  (:body (request client :post :query
                  {:content-type :transit+json
                   :form-params (-> (into {:query query} query-opts)
                                    (update :basis (fn [b] (cond->> b (instance? Basis b) (into {}))))
                                    (time/after-latest-submitted-tx client))
                   :as ::transit+json->result-or-error})))

(defrecord XtdbClient [base-url, !latest-submitted-tx]
  IXtdb
  (^java.util.stream.Stream openQuery [client ^String query ^QueryOptions query-opts]
   (open-query client query (into {:key-fn #xt/key-fn :snake-case-string} query-opts)))

  (^java.util.stream.Stream openQuery [client ^XtqlQuery query ^QueryOptions query-opts]
   (open-query client query (into {:key-fn #xt/key-fn :camel-case-string} query-opts)))

  (submitTx [client opts tx-ops]
    (let [{tx :body} (request client :post :tx
                              {:content-type :transit+json
                               :form-params {:tx-ops (vec tx-ops)
                                             :opts opts}})]

      (swap! !latest-submitted-tx time/max-tx tx)
      tx))

  xtp/PStatus
  (latest-submitted-tx [_] @!latest-submitted-tx)

  (status [client]
    (:body (request client :get :status)))

  AutoCloseable
  (close [_]))

(defn start-client ^java.lang.AutoCloseable [url]
  (->XtdbClient url (atom nil)))
