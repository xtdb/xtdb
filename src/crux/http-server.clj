(ns crux.http-server
  (:require [crux.doc :as doc]
            [crux.query :as q]
            [ring.adapter.jetty :as j]))

(defn handler [kv request]
  (case (:request-method request)
    :get {:status 200
          :headers {"Content-Type" "text/plain"}
          :body "Status: OK"}

    :post {:status 200
           :headers {"Content-Type" "text/plain"}
           :body (str
                  (q/q (doc/db kv) (:body request)))}))

(defn start-endpoint [kv]
  (j/run-jetty (partial handler kv) {:port 3000}))
