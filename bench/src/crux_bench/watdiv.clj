(ns crux-bench.watdiv
  (:require
   [crux.kafka :as k]
   [crux.rdf :as rdf]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.tools.logging :as log])
  (:import [java.io Closeable]
           [java.time Duration]))

(defn load-rdf-into-crux
  [{:keys [^crux.api.ICruxSystem crux] :as runner} resource]
  (let [submit-future (future
                        (with-open [in (io/input-stream (io/resource resource))]
                          (rdf/submit-ntriples (:tx-log crux) in 1000)))]
    (assert (= 521585 @submit-future))
    (.sync crux (Duration/ofSeconds 60))))

(defrecord WatdivRunner [running-future]
  Closeable
  (close [_]
    (future-cancel running-future)))

(defn run-watdiv-test
  [{:keys [crux] :as options}]

  (map->WatdivRunner
    {:running-future
     (future
       (log/info "starting to load watdiv data into crux")
       (load-rdf-into-crux options "watdiv/data/watdiv.10M.nt")
       (log/info "completed loading watdiv data into crux")

       (println "Now it should be starting to run the tests"))}))
