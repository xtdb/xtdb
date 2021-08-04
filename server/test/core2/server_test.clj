(ns core2.server-test
  (:require [clojure.test :as t]
            [core2.log :as log]
            [core2.server :as server]
            [core2.test-util :as tu]
            [hato.client :as hato]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [reitit.core :as r])
  (:import java.time.Duration))

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

(def transit-opts
  {:decode {:handlers server/tj-read-handlers}
   :encode {:handlers server/tj-write-handlers}})

(defn submit-tx [tx-ops]
  (-> (hato/post (url-for :tx)
                 {:accept :transit+json
                  :as :transit+json
                  :content-type :transit+json
                  :form-params tx-ops
                  :transit-opts transit-opts})
      :body))

(defn query [q-body]
  (-> (hato/post (url-for :query)
                 {:accept :transit+json
                  :as :transit+json
                  :content-type :transit+json
                  :form-params q-body
                  :transit-opts transit-opts})
      :body))

(t/deftest test-simple-query
  (let [tx #core2/tx-instant {:tx-id 0, :tx-time #inst "2020-01-01"}]
    (t/is (= tx (submit-tx [[:put {:_id "foo"}]])))

    (t/is (= [{:id "foo"}]
             (query {:query (-> '{:find [?id]
                                  :where [[?e :_id ?id]]}
                                (assoc :basis {:tx tx}
                                       :basis-timeout (Duration/ofSeconds 1)))})))))
