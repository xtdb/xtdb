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
  (:import [java.io Closeable]))

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


(defn success-response [m]
  {:status 200
   :headers {"Content-Type" "application/edn"}
   :body (pr-str m)})


(defn exception-response [^Exception e status]
  {:status status
   :headers {"Content-Type" "text/plain"}
   :body (str
          (.getMessage e) "\n"
          (ex-data e))})

(defmacro let-valid [bindings expr]
  `(try
     (let ~bindings
       (try
         ~expr
         (catch Exception e#
           (if (st/starts-with? (.getMessage e#) "Invalid input")
             (exception-response e# 400) ;; Valid edn, invalid content
             (exception-response e# 500))))) ;; Valid content; something internal failed, or content validity is not properly checked
     (catch Exception e#
       (exception-response e# 400)))) ;;Invalid edn

;; ---------------------------------------------------
;; Services

(defn status [kvs db-dir]
  (success-response
   {:crux.kv-store/kv-backend (.getName (class kvs))
    :crux.kv-store/estimate-num-keys (kvs/count-keys kvs)
    :crux.kv-store/size (cio/folder-human-size db-dir)
    :crux.tx-log/tx-time (doc/read-meta kvs :crux.tx-log/tx-time)}))

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

(defn handler [kvs tx-log db-dir request]
  (cond
    (check-path request ["/"] [:get])
    (status kvs db-dir)

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
  ([kvs tx-log db-dir]
   (create-server kvs tx-log 3000))

  ([kvs tx-log db-dir port]
   (let [server (j/run-jetty (p/wrap-params (partial handler kvs tx-log db-dir))
                             {:port port
                              :join? false})]
     (println (str "HTTP server started on port " port))
     (reify Closeable (close [_] (.stop server))))))
