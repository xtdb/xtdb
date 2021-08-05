(ns core2.api-test
  (:require [core2.api :as c2]
            [clojure.test :as t]
            [core2.test-util :as tu :refer [*node*]]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [core2.client :as client]
            [core2.log :as log]))

(defmethod ig/init-key ::clock [_ _]
  (tu/->mock-clock))

(defn- with-client [f]
  (let [port (tu/free-port)
        sys (-> {:core2/server {:node *node*, :port port}}
                ig/prep
                (doto ig/load-namespaces)
                ig/init)]
    (try
      (binding [*node* (client/start-client (str "http://localhost:" port))]
        (f))
      (finally
        (ig/halt! sys)))))

(def api-implementations
  (-> {:in-memory tu/with-node
       :remote (t/join-fixtures [tu/with-node with-client])}

      #_(select-keys [:in-memory])
      #_(select-keys [:remote])))

(def ^:dynamic *node-type*)

(defn with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

(t/use-fixtures :each
  (tu/with-opts {::clock {}
                 ::log/memory-log {:clock (ig/ref ::clock)}})
  with-each-api-implementation)

(t/deftest test-status
  (t/is (map? (c2/status tu/*node*))))
