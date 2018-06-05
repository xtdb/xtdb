(ns crux.client
  (:require [crux.query :as q]
   [ring.adapter.jetty :as j]))

(defn handler [db request]
  (case (:request-method request)
    :get {:status 200
          :headers {"Content-Type" "text/plain"}
          :body "Status: OK"}

    :post {:status 200
           :headers {"Content-Type" "text/plain"}
           :body (q/q db (:body request))}))

(defn start-endpoint [db]
  (j/run-jetty (partial handler db) {:port 3000}))
