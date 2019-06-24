(ns crux.fixtures.http-server
  (:require [crux.http-server :as srv]
            [crux.io :as cio]))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)

(defn with-http-server [cluster-node f]
  (let [server-port (cio/free-port)]
    (with-open [http-server (srv/start-http-server cluster-node {:server-port server-port})]
      (binding [*api-url* (str "http://" *host* ":" server-port)]
        (f)))))
