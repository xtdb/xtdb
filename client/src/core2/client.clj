(ns core2.client
  (:require [cognitect.transit :as transit]
            [core2.api :as c2]
            [hato.client :as hato])
  (:import clojure.lang.IReduceInit
           core2.api.TransactionInstant
           java.lang.AutoCloseable
           java.time.Duration
           java.util.concurrent.CompletableFuture
           java.util.function.Function))

(def tj-read-handlers
  {"core2/duration" (transit/read-handler #(Duration/parse %))
   "core2/tx-instant" (transit/read-handler c2/map->TransactionInstant)})

(def tj-write-handlers
  {Duration (transit/write-handler "core2/duration" str)
   TransactionInstant (transit/write-handler "core2/tx-instant" #(into {} %))})

(def transit-opts
  {:decode {:handlers tj-read-handlers}
   :encode {:handlers tj-write-handlers}})

(defrecord Core2Client [base-url]
  c2/PClient
  (plan-query-async [_ query params]
    (let [basis-tx (get-in query [:basis :tx])
          ^CompletableFuture !basis-tx (if (instance? CompletableFuture basis-tx)
                                         basis-tx
                                         (CompletableFuture/completedFuture basis-tx))]
      (-> !basis-tx
          (.thenCompose (reify Function
                          (apply [_ basis-tx]
                            (hato/post (str base-url "/query")
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

  (latest-completed-tx [_]
    (-> (hato/get (str base-url "/latest-completed-tx")
                  {:accept :transit+json
                   :as :transit+json})
        :body))

  c2/PSubmitNode
  (submit-tx [_ tx-ops]
    (-> ^CompletableFuture
        (hato/post (str base-url "/tx")
                   {:accept :transit+json
                    :as :transit+json
                    :content-type :transit+json
                    :form-params tx-ops
                    :transit-opts transit-opts
                    :async? true})

        (.thenApply (reify Function
                      (apply [_ resp]
                        (:body resp))))))

  AutoCloseable
  (close [_]))

(defn start-client ^core2.client.Core2Client [url]
  (->Core2Client url))
