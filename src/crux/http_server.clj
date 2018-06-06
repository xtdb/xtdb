(ns crux.http-server
  (:require [crux.doc :as doc]
            [crux.kv :as kv]
            [crux.query :as q]
            [ring.adapter.jetty :as j]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn handler [kv request]
  (case (:request-method request)
    :get ;; health check
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body "Status: OK"}

    :post ;; Read
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (let [db (doc/db kv)
                 query (read-string (req/body-string request))]
             (str (q/q db query)))}

    :put ;; Write
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (let [tx (read-string (req/body-string request))]
             (kv/-put kv tx)
             (str "Successfully inserted " tx))}))

(defn ^Closeable create-server [kv]
  (let [server (j/run-jetty (partial handler kv)
                            {:port 3000
                             :join? false})]
    (reify Closeable (close [_] (.stop server)))))
