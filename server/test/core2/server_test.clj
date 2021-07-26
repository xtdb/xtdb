(ns core2.server-test
  (:require [core2.server :as server]
            [core2.test-util :as tu]
            [clojure.test :as t]
            [core2.log :as log]
            [hato.client :as hato]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [reitit.core :as r]))

(def ^:private ^:dynamic *port*)
(def ^:private ^:dynamic *server*)

(defn- with-server [f]
  (binding [*port* (tu/free-port)]
    (let [{:keys [:core2/server] :as sys} (-> {:core2/server {:node tu/*node*, :port *port*}}
                                              ig/prep
                                              (doto ig/load-namespaces)
                                              ig/init)]
      (try
        (binding [*server* server]
          (f))
        (finally
          (ig/halt! sys))))))

(defmethod ig/init-key ::clock [_ _]
  (tu/->mock-clock))

(t/use-fixtures :each
  (tu/with-opts {::clock {}
                 ::log/memory-log {:clock (ig/ref ::clock)}})
  tu/with-node
  with-server)

(defn url-for [endpoint]
  (format "http://localhost:%d%s"
          *port*
          (-> (r/match-by-name server/router endpoint)
              r/match->path)))

(defn submit-tx [tx-ops]
  (-> (hato/post (url-for :tx)
                 {:accept :edn
                  :as :clojure
                  :content-type :edn
                  :body (pr-str tx-ops)})
      :body))

(t/deftest test-simple-query
  (t/is (= {:tx-id 0, :tx-time #inst "2020-01-01"}
           (submit-tx [[:put {:_id "foo"}]]))))
