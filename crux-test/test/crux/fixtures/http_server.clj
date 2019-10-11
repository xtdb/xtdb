(ns crux.fixtures.http-server
  (:require [crux.fixtures.api :refer [*api*]]
            [crux.http-server :as srv]
            [crux.io :as cio])
  (:import crux.api.Crux))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)

(defn with-http-server [f]
  (let [server-port (cio/free-port)]
    (with-open [http-server ^java.io.Closeable (srv/start-http-server *api* {:server-port server-port})]
      (binding [*api-url* (str "http://" *host* ":" server-port)]
        (with-open [api-client (Crux/newApiClient *api-url*)]
          (binding [*api* api-client]
            (f)))))))
