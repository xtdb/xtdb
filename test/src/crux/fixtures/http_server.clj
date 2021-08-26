(ns crux.fixtures.http-server
  (:require [crux.fixtures :as fix :refer [*api*]]
            [xtdb.http-server :as srv]
            [crux.io :as cio]
            [crux.api :as api]))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)
(def ^:dynamic *api-client-opts*)

(defn with-http-server [f]
  (let [server-port (cio/free-port)]
    (fix/with-opts {:xtdb.http-server/server {:port server-port}}
      (fn []
        (binding [*api-url* (str "http://" *host* ":" server-port)]
          (f))))))

(defn with-http-client [f]
  (with-open [api-client (api/new-api-client *api-url* *api-client-opts*)]
    (binding [*api* api-client]
      (f))))

(defn with-api-client-opts [client-opts f]
  (binding [*api-client-opts* client-opts]
    (f)))
