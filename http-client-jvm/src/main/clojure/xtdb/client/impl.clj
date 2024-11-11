(ns xtdb.client.impl
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as hato]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.middleware :as hato.middleware]
            [juxt.clojars-mirrors.reitit-core.v0v5v15.reitit.core :as r]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde])
  (:import [java.io EOFException InputStream]
           java.lang.AutoCloseable
           java.net.http.HttpClient
           java.util.Spliterator
           [java.util.stream StreamSupport]))

(def transit-opts
  {:decode {:handlers serde/transit-read-handlers}
   :encode {:handlers serde/transit-write-handlers}})

(def router
  (r/router xtp/http-routes))

(defn- request
  ([client request-method endpoint]
   (request client request-method endpoint {}))

  ([{:keys [base-url http-client]} request-method endpoint opts]
   (try
     (hato/request (merge {:accept :transit+json
                           :as :transit+json
                           :request-method request-method
                           :version :http-1.1
                           :http-client http-client
                           :url (str base-url
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

(defn- open-query [client query {:keys [authn] :as query-opts}]
  (:body (request client :post :query
                  {:content-type :transit+json
                   :basic-auth (zipmap [:user :pass] authn)
                   :form-params (into {:query query, :after-tx-id (xtp/latest-submitted-tx-id client)}
                                      (dissoc query-opts :authn))
                   :as ::transit+json->result-or-error})))

(defrecord XtdbClient [base-url, ^HttpClient http-client, !latest-submitted-tx-id]
  xtp/PNode
  (submit-tx [client tx-ops {:keys [authn] :as opts}]
    (let [{tx-id :body} (request client :post :tx
                                 {:content-type :transit+json
                                  :basic-auth (zipmap [:user :pass] authn)
                                  :form-params {:tx-ops (vec tx-ops)
                                                :opts (dissoc opts :authn)}})]

      (swap! !latest-submitted-tx-id (fnil max tx-id) tx-id)
      tx-id))

  (execute-tx [client tx-ops {:keys [authn] :as opts}]
    (let [{tx-res :body} (request client :post :tx
                                  {:content-type :transit+json
                                   :basic-auth (zipmap [:user :pass] authn)
                                   :form-params {:tx-ops (vec tx-ops)
                                                 :opts (dissoc opts :authn)
                                                 :await-tx? true}})
          tx-id (:tx-id tx-res)]
      (swap! !latest-submitted-tx-id (fnil max tx-id) tx-id)
      tx-res))

  (open-sql-query [client query query-opts]
    (open-query client query (into {:key-fn #xt/key-fn :snake-case-string} query-opts)))

  (open-xtql-query [client query query-opts]
    (open-query client query (into {:key-fn #xt/key-fn :snake-case-string} query-opts)))

  xtp/PStatus
  (latest-submitted-tx-id [_] @!latest-submitted-tx-id)

  (status [client]
    (:body (request client :post :status
                    {:content-type :transit+json
                     :form-params {}})))

  (status [client {:keys [authn] :as opts}]
    (:body (request client :post :status
                    {:content-type :transit+json
                     :basic-auth (zipmap [:user :pass] authn)
                     :form-params {:opts (dissoc opts :authn)}})))

  AutoCloseable
  (close [_]
    (.close http-client)))

(defn start-client ^java.lang.AutoCloseable [url]
  (->XtdbClient url (hato/build-http-client {}) (atom -1)))
