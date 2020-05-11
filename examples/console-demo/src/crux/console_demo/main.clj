(ns crux.console-demo.main
  (:require [crux.api :as api]
            [integrant.core :as ig]
            [integrant.repl :as ir]
            [clojure.java.io :as io])
  (:import java.io.Closeable)
  (:gen-class))

(defmethod ig/init-key :console-demo/crux-node [_ node-opts]
  (let [node (doto (api/start-node node-opts) (api/sync))
        submit-data? (nil? (api/entity (api/db node) :roger-moore))]
    (when submit-data?
      (let [submitted-tx (api/submit-tx node (mapv (fn [e] [:crux.tx/put e])
                                                   (-> (io/resource "data/james-bond.edn")
                                                       (slurp)
                                                       (read-string))))]
        (prn "Loading Sample Data...")
        ;; Await data-set data
        (api/await-tx node submitted-tx)
        ;; Submit :bar multiple times - entity to showcase entity searching/history.
        (api/submit-tx node [[:crux.tx/put {:crux.db/id :bar} #inst "2018-06-01"]])
        (api/submit-tx node [[:crux.tx/put {:crux.db/id :bar :map {:a 1 :b 2}} #inst "2019-04-04"]])
        (api/submit-tx node [[:crux.tx/put {:crux.db/id :bar :vector [:a :b]} #inst "2020-01-02"]])
        (api/await-tx node (api/submit-tx node [[:crux.tx/put {:crux.db/id :bar :hello "world"}]]))

        (prn "Sample Data Loaded!")))
    node))

(defmethod ig/halt-key! :console-demo/crux-node [_ ^Closeable node]
  (.close node))

(defn config []
  {:console-demo/crux-node {:crux.node/topology ['crux.standalone/topology
                                                 'crux.kv.rocksdb/kv-store
                                                 'crux.http-server/module]
                            :crux.http-server/read-only? true}})

(defn -main [& args]
  (ir/set-prep! config)
  (ir/go))
