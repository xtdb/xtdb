(ns crux-bench.watdiv
  (:require
   [crux.kafka :as k]
   [crux.rdf :as rdf]
   [clojure.java.io :as io]
   [crux.sparql :as sparql]
   [clojure.string :as str]
   [clojure.tools.logging :as log])
  (:import [java.io Closeable]
           [java.time Duration]))

(def query-timeout-ms 15000)

(defn load-rdf-into-crux
  [{:keys [^crux.api.ICruxSystem crux] :as runner} resource]
  (let [submit-future (future
                        (with-open [in (io/input-stream (io/resource resource))]
                          (rdf/submit-ntriples (:tx-log crux) in 1000)))]
    (assert (= 521585 @submit-future))
    (.sync crux (Duration/ofSeconds 60))))

(defn lazy-count-with-timeout
  [^crux.api.ICruxSystem crux q]
  (let [query-future (future (count (.q (.db crux) q)))]
    (or (deref query-future query-timeout-ms nil)
        (do (future-cancel query-future)
            (throw (IllegalStateException. "Query timed out."))))))

(defrecord WatdivRunner [running-future]
  Closeable
  (close [_]
    (future-cancel running-future)))

(defn execute-stress-test
  [{:keys [^crux.api.ICruxSystem crux] :as options} tests-run out-file]
  (with-open [desc-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.desc"))
              sparql-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.sparql"))
              out (io/writer out-file)]
    (.write out "[\n")
    (doseq [[idx [d q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                             (take 100)
                             (map-indexed vector))]
      (.write out "{")
      (.write out (str ":idx " (pr-str idx) "\n"))
      (.write out (str ":query " (pr-str q) "\n"))
      (let [start-time (System/currentTimeMillis)]
        (try
          (.write out (str ":crux-results " (lazy-count-with-timeout crux (sparql/sparql->datalog q))
                           "\n"))
          (catch IllegalStateException t
            (.write out (str ":crux-error " (pr-str (str t)) "\n")))
          (catch Throwable t
            (.write out (str ":crux-error " (pr-str (str t)) "\n"))
            (throw t)))
        (.write out (str ":crux-time " (pr-str (-  (System/currentTimeMillis) start-time)))))

      (.write out "}\n")
      (.flush out)
      (swap! tests-run inc))
    (.write out "]")))

(defn run-watdiv-test
  [{:keys [^crux.api.ICruxSystem crux] :as options}]
  (let [status (atom nil)
        tests-run (atom 0)
        out-file (io/file (format "target/watdiv_%s.edn" (System/currentTimeMillis)))]
    (map->WatdivRunner
      {:status status
       :tests-run tests-run
       :out-file out-file
       :running-future
       (future
         (try
           (when-not (:done? (.entity (.db crux) ::watdiv-injestion-status))
             (log/info "starting to load watdiv data into crux")
             (reset! status :injesting-watdiv-data)
             (load-rdf-into-crux options "watdiv/data/watdiv.10M.nt")
             (log/info "completed loading watdiv data into crux")
             (.submitTx crux [[:crux.tx/put
                               ::watdiv-injestion-status
                               {:crux.db/id ::watdiv-injestion-status
                                :done? true}]]))
           (reset! status :running-benchmark)
           (execute-stress-test options tests-run out-file)
           (reset! status :benchmark-completed)
           (catch Throwable t
             (reset! status :benchmark-failed)
             false)))})))
