(ns crux.http-server
  (:require [crux.doc :as doc]
            [crux.query :as q]
            [ring.adapter.jetty :as j]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn handler [kv request]
  (case (:request-method request)
    :get {:status 200
          :headers {"Content-Type" "text/plain"}
          :body "Status: OK"}

    :post {:status 200
           :headers {"Content-Type" "text/plain"}
           :body (let [db (doc/db kv)
                       query (req/body-string request)]
                   (str (q/q db query)))}))

(defn create-server [kv]
  (let [server (j/run-jetty (partial handler kv)
                            {:port 3000
                             :join? false})]
    (reify Closeable (close [_] (.stop server)))))
