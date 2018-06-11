(ns crux.http-server
  (:require [clojure.edn :as edn]
            [crux.db :as db]
            [crux.doc :as doc]
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

(defn on-get [kvs] ;; Health check
  {:status 200
   :headers {"Content-Type" "application/edn"}
   :body (pr-str
          {:kv-backend (type kvs)
           :estimate-num-keys (kvs/count-keys kvs)})})

(defn on-post [kvs request] ;; Read
  (try
    (let [query (edn/read-string (req/body-string request))]
      (try
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str (q/q (doc/db kvs) query))}
        (catch Exception e
          (if (= "Invalid input" (.getMessage e))
            (exception-response e 400)
            (exception-response e 500)))))
    (catch Exception e
      (exception-response e 400))))

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
      (exception-response e 500))))

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
