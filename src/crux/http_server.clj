(ns crux.http-server
  (:require [clojure.edn :as edn]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.io :as cio]
            [crux.tx :as tx]
            [crux.kv-store :as kvs]
            [crux.query :as q]
            [ring.adapter.jetty :as j]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn exception-response [^Exception e status]
  {:status status
   :headers {"Content-Type" "text/plain"}
   :body (str
          (.getMessage e) "\n"
          (ex-data e))})

(defn on-get [kvs db-dir] ;; Health check
  {:status 200
   :headers {"Content-Type" "application/edn"}
   :body (pr-str
          {:kv-backend (type kvs)
           :estimate-num-keys (kvs/count-keys kvs)
           :db-dir-size (cio/folder-human-size db-dir)
           :dev-db-size (cio/folder-human-size "dev-storage")})})

(defn on-post [kvs request] ;; Read
  (try
    (let [query (edn/read-string (req/body-string request))]
      (try
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str (q/q (doc/db kvs) query))}
        (catch Exception e
          (if (= "Invalid input" (.getMessage e))
            (exception-response e 400) ;; Valid edn, invalid query
            (exception-response e 500))))) ;; Valid query; something internal failed
    (catch Exception e
      (exception-response e 400)))) ;; Invalid edn

(defn on-put [kvs tx-log request] ;; Write
  (try
    (let [v (read-string (req/body-string request))
          tx-op [:crux.tx/put
                 (java.util.UUID/randomUUID)
                 v
                 (java.util.Date.)] ;; WIP; ought to use v directly and assume user provided valid tx-op syntax
          return (db/submit-tx tx-log [tx-op])]      
      {:status 200
       :headers {"Content-Type" "application/edn"}
       :body (pr-str @return)})
    (catch Exception e
      (exception-response e 500)))) ;; TODO invalid requests need a 400

(defn handler [kvs tx-log db-dir request]
  (case (:request-method request)
    :get (on-get kvs db-dir)
    :post (on-post kvs request)
    :put (on-put kvs tx-log request)))

(defn ^Closeable create-server
  ([kvs tx-log db-dir]
   (create-server kvs tx-log 3000))

  ([kvs tx-log db-dir port]
   (let [server (j/run-jetty (partial handler kvs tx-log db-dir)
                             {:port port
                              :join? false})]
     (reify Closeable (close [_] (.stop server))))))
