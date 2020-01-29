(ns crux.fixtures.http-server
  (:require [crux.fixtures.api :refer [*api*]]
            [crux.http-server :as srv]
            [crux.io :as cio]
            [crux.fixtures.api :as fapi])
  (:import crux.api.Crux))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)

(defn with-http-server [f]
  (let [server-port (cio/free-port)]
    (fapi/with-opts (-> fapi/*opts*
                        (update :crux.node/topology conj 'crux.http-server/module)
                        (assoc :crux.http-server/port server-port))
      (fn []
        (binding [*api-url* (str "http://" *host* ":" server-port)]
          (f))))))

(defn with-http-client [f]
  (with-open [api-client (Crux/newApiClient *api-url*)]
    (binding [*api* api-client]
      (f))))
