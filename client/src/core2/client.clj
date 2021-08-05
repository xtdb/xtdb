(ns core2.client
  (:require [core2.api :as c2]
            [core2.transit :as c2.transit]
            [hato.client :as hato]
            [reitit.core :as r])
  (:import clojure.lang.IReduceInit
           java.lang.AutoCloseable
           java.util.concurrent.CompletableFuture
           java.util.function.Function))

(def transit-opts
  {:decode {:handlers c2.transit/tj-read-handlers}
   :encode {:handlers c2.transit/tj-write-handlers}})

(def router
  (r/router c2/http-routes))

(defn url-for [{:keys [base-url]} endpoint]
  (str base-url (-> (r/match-by-name router endpoint)
                    (r/match->path))))

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
                            (hato/post (url-for client :query)
                                       {:accept :transit+json
                                        :as :transit+json
                                        :content-type :transit+json
                                        :form-params {:query (-> query
                                                                 (assoc-in [:basis :tx] basis-tx))
                                                      :params params}
                                        :transit-opts transit-opts
                                        :async? true}))))
          (.thenApply (reify Function
                        (apply [_ resp]
                          (reify IReduceInit
                            (reduce [_ f init]
                              (reduce f init (:body resp))))))))))

  (latest-completed-tx [client]
    (-> (hato/get (url-for client :latest-completed-tx)
                  {:accept :transit+json
                   :as :transit+json})
        :body))

  c2/PSubmitNode
  (submit-tx [client tx-ops]
    (-> ^CompletableFuture
        (hato/post (url-for client :tx)
                   {:accept :transit+json
                    :as :transit+json
                    :content-type :transit+json
                    :form-params tx-ops
                    :transit-opts transit-opts
                    :async? true})

        (.thenApply (reify Function
                      (apply [_ resp]
                        (:body resp))))))

  c2/PStatus
  (status [client]
    (-> (hato/get (url-for client :status)
                  {:accept :transit+json
                   :as :transit+json})
        :body))

  AutoCloseable
  (close [_]))

(defn start-client ^core2.client.Core2Client [url]
  (->Core2Client url))
