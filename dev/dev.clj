(ns dev
  (:require [clojure.java.io :as io]
            [core2.core :as c2]
            [core2.tpch :as tpch]
            [core2.tpch-test :as tpch-test]
            [core2.util :as util]
            [integrant.core :as i]
            [integrant.repl :as ir]
            [core2.test-util :as tu])
  (:import [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory))

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (when level
               (Level/valueOf (name level)))))

(defn get-log-level! [ns]
  (some->> (.getLevel ^Logger (LoggerFactory/getLogger (name ns)))
           (str)
           (.toLowerCase)
           (keyword)))

(defmacro with-log-level [ns level & body]
  `(let [level# (get-log-level! ~ns)]
     (try
       (set-log-level! ~ns ~level)
       ~@body
       (finally
         (set-log-level! ~ns level#)))))

(def dev-node-dir
  (io/file "dev/dev-node"))

(def node)

(defmethod i/init-key ::crux [_ {:keys [node-opts]}]
  (alter-var-root #'node (constantly (c2/start-node node-opts)))
  node)

(defmethod i/halt-key! ::crux [_ node]
  (util/try-close node)
  (alter-var-root #'node (constantly nil)))

(def standalone-config
  {::crux {:node-opts {:core2/log {:core2/module 'core2.log/->local-directory-log
                                   :root-path (io/file dev-node-dir "log")}
                       :core2/buffer-pool {:cache-path (io/file dev-node-dir "buffers")}
                       :core2/object-store {:core2/module 'core2.object-store/->file-system-object-store
                                            :root-path (io/file dev-node-dir "objects")}}}})

(ir/set-prep! (fn [] standalone-config))

(def go ir/go)
(def halt ir/halt)
(def reset ir/reset)

(comment
  (def !submit-tpch
    (future
      (time
       (tpch/submit-docs! node 0.1))))

  (with-open [wm (c2/open-watermark node)
              res (c2/open-q node wm tpch/tpch-q5-local-supplier-volume)]
    (time
     (count (into [] (mapcat seq (tu/<-cursor res)))))))
