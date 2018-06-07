(ns crux.http-server
  (:require [clojure.edn :as edn]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.doc.tx :as tx]
            [crux.kv-store :as kvs]
            [crux.query :as q] 
            [ring.adapter.jetty :as j]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn exception-response [^Exception e]
  {:status 400
   :headers {"Content-Type" "text/plain"}
   :body (str
          (.getMessage e) "\n"
          (ex-data e))})

(defn on-get [kvs] ;; Health check
  {:status 200
   :headers {"Content-Type" "application/edn"}
   :body (pr-str
          {:estimate-num-keys (kvs/count-keys kvs)})})

(defn on-post [kvs request] ;; Read
  (try
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (let [db (doc/db kvs)
                 query (edn/read-string (req/body-string request))]
             (pr-str (q/q db query)))}
    (catch Exception e
      (exception-response e))))

(defn on-put [kvs request] ;; Write
  (try
    (let [v (read-string (req/body-string request))
          tx-op [:crux.tx/put
                 (java.util.UUID/randomUUID)
                 v
                 (java.util.Date.)]]
      (db/submit-tx
       (tx/->DocTxLog kvs)
       [tx-op])
      {:status 200
       :headers {"Content-Type" "text/plain"}
       :body (str "Successfully inserted " v)})
    (catch Exception e
      (exception-response e))))

(defn handler [kvs request]
  (case (:request-method request)
    :get (on-get kvs)
    :post (on-post kvs request)
    :put (on-put kvs request)))

(defn ^Closeable create-server
  ([kvs]
   (create-server kvs 3000))
  
  ([kvs port]
   (let [server (j/run-jetty (partial handler kvs)
                             {:port port
                              :join? false})]
     (reify Closeable (close [_] (.stop server))))))
