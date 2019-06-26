(ns crux.fixtures.bootstrap
  (:require [clojure.test :as t]
            [crux.fixtures.http-server :refer [*api-url*]]
            [crux.fixtures.kafka
             :refer
             [*doc-topic* *kafka-bootstrap-servers* *tx-topic*]]
            [crux.fixtures.kv :refer [*kv* *kv-backend*]]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI]
           java.util.UUID))

(def ^:dynamic ^ICruxAPI *api*)

(defn with-cluster-node [f]
  (assert (bound? #'*kafka-bootstrap-servers*))
  (assert (not (bound? #'*kv*)))
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        test-id (UUID/randomUUID)]
    (binding [*tx-topic* (str "tx-topic-" test-id)
              *doc-topic* (str "doc-topic-" test-id)]
      (try
        (with-open [cluster-node (Crux/startClusterNode {:db-dir db-dir
                                                         :kv-backend *kv-backend*
                                                         :tx-topic *tx-topic*
                                                         :doc-topic *doc-topic*
                                                         :bootstrap-servers *kafka-bootstrap-servers*})]
          (binding [*api* cluster-node]
            (f)))
        (finally
          (cio/delete-dir db-dir))))))

(defn with-standalone-system [f]
  (assert (not (bound? #'*kv*)))
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (with-open [standalone-system (Crux/startStandaloneSystem {:db-dir db-dir
                                                                 :kv-backend *kv-backend*
                                                                 :event-log-dir event-log-dir})]
        (binding [*api* standalone-system]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir event-log-dir)))))

(defn with-api-client [f]
  (assert (bound? #'*api-url*))
  (with-open [api-client (Crux/newApiClient *api-url*)]
    (binding [*api* api-client]
      (f))))

(defn with-each-api-implementation [f]
  (t/testing "Local API ClusterNode"
    (with-cluster-node f))
  (t/testing "Local API StandaloneSystem"
    (with-standalone-system f))
  (t/testing "Remote API"
    (with-cluster-node
      (fn []
        (crux.fixtures.http-server/with-http-server *api*
          (fn []
            (with-api-client f)))))))
