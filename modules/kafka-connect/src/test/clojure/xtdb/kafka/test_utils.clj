;; TODO: Move to xtdb.test-util
(ns xtdb.kafka.test-utils
  (:require [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (java.net ServerSocket)
           (xtdb.kafka.connect XtdbSinkConfig)))

(def ^:dynamic *node-opts* {})
#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(defn with-node [f]
  #_{:clj-kondo/ignore [:unresolved-symbol]}
  (util/with-open [node (xtn/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn ->config [config]
  (XtdbSinkConfig/parse config))

(comment
  (->config {"url" "test"
             "id.mode" "record_key"}))
