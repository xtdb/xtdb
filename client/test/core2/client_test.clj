(ns core2.client-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.client :as client]
            [core2.log :as log]
            [core2.test-util :as tu]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import java.time.Duration))

(defn- with-client [f]
  (let [port (tu/free-port)
        sys (-> {:core2/server {:node tu/*node*, :port port}}
                ig/prep
                (doto ig/load-namespaces)
                ig/init)]
    (try
      (binding [tu/*node* (client/start-client (str "http://localhost:" port))]
        (f))
      (finally
        (ig/halt! sys)))))

(defmethod ig/init-key ::clock [_ _]
  (tu/->mock-clock))

(t/use-fixtures :each
  (tu/with-opts {::clock {}
                 ::log/memory-log {:clock (ig/ref ::clock)}})
  tu/with-node
  with-client)

(t/deftest test-simple-query
  (let [!tx (c2/submit-tx tu/*node* [[:put {:_id "foo"}]])]
    (t/is (= #core2/tx-instant {:tx-id 0, :tx-time #inst "2020-01-01"} @!tx))

    (t/is (= [{:id "foo"}]
             (->> (c2/plan-query tu/*node* (-> '{:find [?id]
                                                 :where [[?e :_id ?id]]}
                                               (assoc :basis {:tx !tx}
                                                      :basis-timeout (Duration/ofSeconds 1))))
                  (into []))))))
