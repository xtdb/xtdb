(ns crux.java-api-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.fixtures :as fix]
            [crux.kv :as kv])
  (:import [crux.api Crux ICruxAsyncIngestAPI ICruxAPI ModuleConfigurator NodeConfigurator]
           java.util.function.Consumer))

(defmacro consume {:style/indent 1} [[binding] & body]
  `(reify Consumer
     (~'accept [_# ~binding]
      ~@body)))

(defn start-rocks-node ^ICruxAPI [data-dir]
  (Crux/startNode
   (consume [c]
     (doto ^NodeConfigurator c
       (.with "crux/tx-log"
              (consume [c]
                (doto ^ModuleConfigurator c
                  (.with "kv-store"
                         (consume [c]
                           (doto ^ModuleConfigurator c
                             (.module "crux.rocksdb/->kv-store")
                             (.set "db-dir" (io/file data-dir "txs"))))))))
       (.with "crux/document-store"
              (consume [c]
                (doto ^ModuleConfigurator c
                  (.with "kv-store"
                         (consume [c]
                           (doto ^ModuleConfigurator c
                             (.module "crux.rocksdb/->kv-store")
                             (.set "db-dir" (io/file data-dir "docs"))))))))
       (.with "crux/index-store"
              (consume [c]
                (doto ^ModuleConfigurator c
                  (.with "kv-store"
                         (consume [c]
                           (doto ^ModuleConfigurator c
                             (.module "crux.rocksdb/->kv-store")
                             (.set "db-dir" (io/file data-dir "indexes"))))))))))))

(defn start-rocks-ingest-node ^ICruxAsyncIngestAPI [data-dir]
  (Crux/newIngestClient
   (consume [c]
     (doto ^NodeConfigurator c
       (.with "crux/document-store"
              (consume [c]
                (doto ^ModuleConfigurator c
                  (.with "kv-store"
                         (consume [c]
                           (doto ^ModuleConfigurator c
                             (.module "crux.rocksdb/->kv-store")
                             (.set "db-dir" (io/file data-dir "docs"))))))))))))

(t/deftest test-configure-rocks
  (fix/with-tmp-dirs #{data-dir}
    (with-open [node (start-rocks-node data-dir)]
      (t/is (= "crux.rocksdb.RocksKv"
               (kv/kv-name (get-in node [:node :tx-log :kv-store]))
               (kv/kv-name (get-in node [:node :document-store :document-store :kv-store]))
               (kv/kv-name (get-in node [:node :index-store :kv-store]))))
      (t/is (= (.toPath (io/file data-dir "txs"))
               (get-in node [:node :tx-log :kv-store :db-dir]))))))

(t/deftest test-configure-rocks-ingest
  (fix/with-tmp-dirs #{data-dir}
    (with-open [node (start-rocks-ingest-node data-dir)]
      (t/is (= "crux.rocksdb.RocksKv"
               (kv/kv-name (get-in node [:client :document-store :document-store :kv-store])))))))