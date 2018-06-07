(ns crux.http-server
  (:require [clojure.edn :as edn]
            [crux.doc :as doc]
            [crux.query :as q] 
            [ring.adapter.jetty :as j]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn on-post [kv request]
  (try
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (let [db (doc/db kv)
                 query (edn/read-string (req/body-string request))]
             (pr-str (q/q db query)))}
    (catch Exception e
      {:status 400
       :headers {"Content-Type" "text/plain"}
       :body (str (if (some? (ex-data e))
                    (ex-data e)
                    (.getMessage e)))})))

(defn handler [kv request]
  (case (:request-method request)
    :get ;; health check
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body "Status: OK"}

    :post ;; Read
    (on-post kv request)

    :put ;; Write
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (let [tx (read-string (req/body-string request))]
;;             (kv/-put kv tx) TODO
             (str "Successfully inserted " tx))}))

(defn ^Closeable create-server
  ([kv]
   (create-server kv 3000))
  
  ([kv port]
   (let [server (j/run-jetty (partial handler kv)
                             {:port port
                              :join? false})]
     (reify Closeable (close [_] (.stop server))))))
