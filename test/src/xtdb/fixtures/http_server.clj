(ns xtdb.fixtures.http-server
  (:require [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.http-server :as srv]
            [xtdb.io :as xio]
            [xtdb.api :as xt]))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)
(def ^:dynamic *api-client-opts*)

(defn with-http-server [f]
  (let [server-port (xio/free-port)]
    (fix/with-opts {:xtdb.http-server/server {:port server-port}}
      (fn []
        (binding [*api-url* (str "http://" *host* ":" server-port)]
          (f))))))

(defn with-http-client [f]
  (with-open [api-client (xt/new-api-client *api-url* *api-client-opts*)]
    (binding [*api* api-client]
      (f))))

(defn with-api-client-opts [client-opts f]
  (binding [*api-client-opts* client-opts]
    (f)))
