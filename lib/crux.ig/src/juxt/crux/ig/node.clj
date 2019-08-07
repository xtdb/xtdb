(ns juxt.crux.ig.node
  (:require [clojure.java.io :as io]
            crux.api
            [crux.http-server :as srv]
            [integrant.core :as ig])
  (:import java.nio.file.attribute.FileAttribute
           java.nio.file.Files))

(defn- delete-directory
  "Delete a directory and all files within"
  [f]
  (run! io/delete-file (filter #(.isFile %) (file-seq f)))
  (run! io/delete-file (reverse (file-seq f))))

(def ^:private tmp-dir
  ;; tools refresh wipes this out, to which I don't have an easy solution.
  ;; I could move it to another namespace which has reloading disabled perhaps.
  (memoize
   (fn [identity]
     (let [path (Files/createTempDirectory nil (into-array FileAttribute []))]
       (.addShutdownHook
         (Runtime/getRuntime)
         (new Thread (fn [] (delete-directory (.toFile path)))))
       (.mkdir (.toFile path))
       (str path)))))

(declare nhttp)

(defmethod ig/halt-key! :juxt.crux.ig/node
  [_ node]
  (.close nhttp)
  (.close node))

(defmethod ig/prep-key :juxt.crux.ig/node
  [k opts]
  (cond-> opts
    (not (contains? opts :event-log-dir))
    (assoc :event-log-dir (tmp-dir [k "event-log"]))
    (not (contains? opts :db-dir))
    (assoc :db-dir (tmp-dir [k "db-dir"]))))

(derive ::standalone :juxt.crux.ig/node)

(defmethod ig/init-key ::standalone
  [_ opts]
  (let [n (crux.api/start-standalone-node opts)
        res-opts
        (assoc (:http-opts n)
          :server-port 8080
          :cors-access-control
          [:access-control-allow-origin [#".*"]
           :access-control-allow-headers ["X-Requested-With"
                                          "Content-Type"
                                          "Cache-Control"
                                          "Origin"
                                          "Accept"
                                          "Authorization"
                                          "X-Custom-Header"]
           :access-control-allow-methods [:get :options :head :post]])
        nhttp (srv/start-http-server n res-opts)]
    (println "The Crux demo HTTP API is now available at http://localhost:8080")
    (def nhttp nhttp)
    n))

(derive ::cluster-node :juxt.crux.ig/node)

(defmethod ig/init-key ::cluster-node
  [_ opts]
  (crux.api/start-cluster-node opts))
