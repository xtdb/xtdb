(ns crux.google.cloud-storage
  (:require [crux.system :as sys]
            [crux.document-store :as ds]
            [crux.checkpoint :as cp]))

(defn ->document-store {::sys/deps (::sys/deps (meta #'ds/->nio-document-store))
                        ::sys/args (::sys/args (meta #'ds/->nio-document-store))}
  [opts]
  (ds/->nio-document-store opts))

(defn ->checkpoint-store {::sys/deps (::sys/deps (meta #'cp/->filesystem-checkpoint-store))
                          ::sys/args (::sys/args (meta #'cp/->filesystem-checkpoint-store))}
  [opts]
  (cp/->filesystem-checkpoint-store opts))
