(ns azure-testing
  (:require [clojure.java.io :as io]
            [xtdb.node :as xtdb]
            [xtdb.util :as util]
            [xtdb.azure :as azure]
            [integrant.core :as i]
            [integrant.repl :as ir])
  (:import java.time.Duration
           [com.azure.storage.common StorageSharedKeyCredential]
           [com.azure.identity DefaultAzureCredentialBuilder]))

(def dev-node-dir
  (io/file "dev/azure-node"))

(def node)
(def storage-account "xtdb")
(def container "xtdb-azure-test")

(defmethod i/init-key ::xtdb [_ {:keys [node-opts]}]
  (alter-var-root #'node (constantly (xtdb/start-node node-opts)))
  node)

(defmethod i/halt-key! ::xtdb [_ node]
  (util/try-close node)
  (alter-var-root #'node (constantly nil)))

(def azure-default-auth
  {::xtdb {:node-opts {:xtdb.log/local-directory-log {:root-path (io/file dev-node-dir "log")}
                       :xtdb.buffer-pool/buffer-pool {:cache-path (io/file dev-node-dir "buffers")}
                       ::azure/blob-object-store {:storage-account storage-account
                                                  :container container}}}})

(ir/set-prep! (fn [] azure-default-auth))

(comment
  (ir/go)
  (node)
  (.putObject (:xtdb.azure/blob-object-store (:system node)) "bar" (java.nio.ByteBuffer/wrap (.getBytes "helloworld")))
  (ir/halt)
  (ir/reset)
  )

(System/getenv "AZURE_TENANT_ID")
(System/getenv "AZURE_CLIENT_ID")
(System/getenv "AZURE_CLIENT_SECRET")
