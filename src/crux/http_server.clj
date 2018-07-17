(ns crux.http-server
  (:require [clojure.edn :as edn]
            [clojure.string :as st]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.io :as cio]
            [crux.tx :as tx]
            [crux.query :as q]
            [crux.kv-store :as kvs]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req])
  (:import [java.io Closeable]
           [org.apache.kafka.clients.consumer
            KafkaConsumer]))

;; ---------------------------------------------------
;; Utils

(defn uuid-str? [s]
  (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(defn sha1-20-str? [s]
  (re-matches #"[a-f0-9]{20}" s))

(defn read-unknown [s]
  (cond
    (uuid-str? s)
    (java.util.UUID/fromString s)
    
    (sha1-20-str? s)
    s
    
    (try (edn/read-string s)
         (catch Exception e false))
    (edn/read-string s)
    
    :default
    (keyword s)))

(defn param
  ([request]
   (param request nil))
  ([request param-name]
   (case (:request-method request)
     :get (-> request
              :query-params
              (find param-name)
              last
              read-unknown)
     :post (-> request
               req/body-string
               edn/read-string))))

(defn check-path [request valid-paths valid-methods]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (some #{path} valid-paths)
         (some #{method} valid-methods))))

(defn response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn success-response [m]
  (response 200
            {"Content-Type" "application/edn"}
            (pr-str m)))

(defn exception-response [status ^Exception e]
  (response status
            {"Content-Type" "text/plain"}
            (str
             (.getMessage e) "\n"
             (ex-data e))))

(defmacro let-valid [bindings expr]
  `(try
     (let ~bindings
       (try
         ~expr
         (catch Exception e#
           (if (st/starts-with? (.getMessage e#) "Invalid input")
             (exception-response 400 e#) ;; Valid edn, invalid content
             (exception-response 500 e#))))) ;; Valid content; something internal failed, or content validity is not properly checked
     (catch Exception e#
       (exception-response 400 e#)))) ;;Invalid edn

(defn zk-active? [^KafkaConsumer consumer]
  (boolean (.listTopics consumer)))

;; ---------------------------------------------------
;; Services

(defn status [kvs db-dir consumer]
  (let [zk-status (zk-active? consumer)
        status-map {:crux.zk/zk-active? zk-status
                    :crux.kv-store/kv-backend (.getName (class kvs))
                    :crux.kv-store/estimate-num-keys (kvs/count-keys kvs)
                    :crux.kv-store/size (cio/folder-human-size db-dir)
                    :crux.tx-log/tx-time (doc/read-meta kvs :crux.tx-log/tx-time)}]
    (if zk-status
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn document [kvs request]
  (let-valid [object-store (doc/->DocObjectStore kvs)
              content-hash (param request "hash")]
    (success-response
     (db/get-objects object-store [content-hash])))) ;;TODO doesn't work

;; param must be compatible with index/id->bytes (e.g. keyworded UUID)
(defn history [kvs request]
  (let-valid [snapshot (kvs/new-snapshot kvs)
              entity (param request "entity")]
    (success-response
     (doc/entity-history snapshot entity))))

(defn query [kvs request]
  (let-valid [query-map (param request "q")]
    (success-response
     (q/q (q/db kvs) query-map))))

(defn transact [tx-log request]
  (let-valid [tx-op (param request)]
    (success-response
     @(db/submit-tx tx-log [tx-op]))))

;; ---------------------------------------------------
;; Jetty server

(defn handler [kvs tx-log db-dir consumer request]
  (cond
    (check-path request ["/"] [:get])
    (status kvs db-dir consumer)

    (check-path request ["/d" "/document"] [:get :post])
    (document kvs request)

    (check-path request ["/h" "/history"] [:get :post])
    (history kvs request)

    (check-path request ["/q" "/query"] [:get :post])
    (query kvs request)

    (check-path request ["/tx-log"] [:post])
    (transact tx-log request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(defn ^Closeable create-server
  ([kvs tx-log db-dir consumer port]
   (let [server (j/run-jetty (p/wrap-params (partial handler kvs tx-log db-dir consumer))
                             {:port port
                              :join? false})]
     (println (str "HTTP server started on port " port))
     (reify Closeable (close [_] (.stop server))))))
