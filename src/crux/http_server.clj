(ns crux.http-server
  (:require [clojure.edn :as edn]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.io :as cio]
            [crux.tx :as tx]
            [crux.kv-store :as kvs]
            [crux.query :as q]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req])
  (:import [java.io Closeable]))

(defn exception-response [^Exception e status]
  {:status status
   :headers {"Content-Type" "text/plain"}
   :body (str
          (.getMessage e) "\n"
          (ex-data e))})

(defn status [kvs db-dir]
  {:status 200
   :headers {"Content-Type" "application/edn"}
   :body (pr-str
          {:kv-backend (type kvs)
           :estimate-num-keys (kvs/count-keys kvs)
           :kv-store-size (cio/folder-human-size db-dir)
           :dev-db-size (cio/folder-human-size "dev-storage")})})

(defn do-query [kvs request]
  (try
    (let [query (case (:request-method request)
                  :get (-> request
                           :query-params
                           (find "q")
                           last
                           edn/read-string)
                  :post (edn/read-string (req/body-string request)))]
      (try
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str (q/q (doc/db kvs) query))}
        (catch Exception e
          (if (= "Invalid input" (.getMessage e))
            (exception-response e 400) ;; Valid edn, invalid query
            (exception-response e 500))))) ;; Valid query; something internal failed
    (catch Exception e
      (exception-response e 400))))  ;; Invalid edn

(defn transact [tx-log request]
  (try
    (let [v (edn/read-string (req/body-string request))
          tx-op [:crux.tx/put
                 (java.util.UUID/randomUUID)
                 v
                 (java.util.Date.)]] ;; WIP; ought to use v directly and assume user provided valid tx-op syntax
      (try        
        {:status 200
         :headers {"Content-Type" "application/edn"}
         :body (pr-str @(db/submit-tx tx-log [tx-op]))}
        (catch Exception e
          (if (= "Invalid input" (.getMessage e))
            (exception-response e 400) ;; Valid edn, invalid tx-op
            (exception-response e 500))))) ;; Valid tx-op; something internal failed
    (catch Exception e
      (exception-response e 400)))) ;; Invalid edn

(defn check-path [request valid-paths valid-methods]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (some #{path} valid-paths)
         (some #{method} valid-methods))))

(defn handler [kvs tx-log db-dir request]
  (cond
    (check-path request ["/"] [:get])
    (status kvs db-dir)

    (check-path request ["/q" "/query"] [:get :post])
    (do-query kvs request)

    (check-path request ["/tx-log"] [:post])
    (transact tx-log request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(defn ^Closeable create-server
  ([kvs tx-log db-dir]
   (create-server kvs tx-log 3000))

  ([kvs tx-log db-dir port]
   (let [server (j/run-jetty (p/wrap-params (partial handler kvs tx-log db-dir))
                             {:port port
                              :join? false})]
     (reify Closeable (close [_] (.stop server))))))
