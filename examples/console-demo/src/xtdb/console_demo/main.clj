(ns xtdb.console-demo.main
  (:require [xtdb.api :as xt]
            [integrant.core :as ig]
            [integrant.repl :as ir]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [xtdb.rocksdb :as rocks])
  (:import java.io.Closeable)
  (:gen-class))

(defmethod ig/init-key :console-demo/xtdb-node [_ node-opts]
  (let [node (doto (xt/start-node node-opts) (xt/sync))
        submit-data? (nil? (xt/entity (xt/db node) :tmdb/cast-65731))]
    (when submit-data?
      (with-open [dataset-rdr (io/reader "https://crux-data.s3.eu-west-2.amazonaws.com/kaggle-tmdb-movies.edn")]
        (let [last-tx (->> (line-seq dataset-rdr)
                           (partition-all 1000)
                           (reduce (fn [last-tx docs-chunk]
                                     (xt/submit-tx node (mapv read-string docs-chunk)))
                                   nil))]
          (prn "Loading Sample Data...")
          ;; Await data-set data
          (xt/await-tx node last-tx)
          ;; Submit :bar multiple times - entity to showcase entity searching/history.
          (xt/submit-tx node [[:xt/put {:xt/id :bar} #inst "2018-06-01"]])
          (xt/submit-tx node [[:xt/put {:xt/id :bar :map {:a 1 :b 2}} #inst "2019-04-04"]])
          (xt/submit-tx node [[:xt/put {:xt/id :bar :vector [:a :b]} #inst "2020-01-02"]])
          (xt/await-tx node (xt/submit-tx node [[:xt/put {:xt/id :bar :hello "world"}]]))

          (prn "Sample Data Loaded!"))))
    node))

(defmethod ig/halt-key! :console-demo/xtdb-node [_ ^Closeable node]
  (.close node))

(defn config []
  {:console-demo/xtdb-node {:xtdb/tx-log {:kv-store {:xtdb/module `rocks/->kv-store, :db-dir "data/tx-log"}}
                            :xtdb/document-store {:kv-store {:xtdb/module `rocks/->kv-store, :db-dir "data/doc-store"}}
                            :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store, :db-dir "data/indices"}}
                            :xtdb.http-server/server {:read-only? true
                                                      :server-label "Console Demo"}
                            :xtdb.metrics/registry {}}})

(defn -main [& args]
  (ir/set-prep! config)
  (ir/go))
