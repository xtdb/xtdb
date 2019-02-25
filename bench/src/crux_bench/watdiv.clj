(ns crux-bench.watdiv
  (:require [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql])
  (:import [java.io Closeable File]
           [com.amazonaws.services.s3.model CannedAccessControlList]
           java.time.Duration))

(def query-timeout-ms 15000)

(defn load-rdf-into-crux
  [{:keys [^crux.api.ICruxSystem crux] :as runner} resource]
  (let [submit-future (future
                        (with-open [in (io/input-stream (io/resource resource))]
                          (rdf/submit-ntriples (:tx-log crux) in 1000)))]
    (assert (= 521585 @submit-future))
    (.submitTx crux [[:crux.tx/put
                      ::watdiv-injestion-status
                      {:crux.db/id ::watdiv-injestion-status
                       :done? true}]])
    (.sync crux (Duration/ofSeconds 6000))))

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
  [{:keys [^crux.api.ICruxSystem crux] :as options} tests-run out-file num-tests]
  (with-open [desc-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.desc"))
              sparql-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.sparql"))
              out (io/writer out-file)]
    (.write out "[\n")
    (doseq [[idx [d q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                             (take (or num-tests 100))
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

(defn upload-watdiv-results
  [^File out-file]
  (s3/put-object
    :bucket-name (System/getenv "CRUX_BENCHMARK_BUCKET")
    :key (.getName out-file)
    :acl :public-read
    :file out-file)
  (s3/set-object-acl
    (System/getenv "CRUX_BENCHMARK_BUCKET")
    (.getName out-file)
    CannedAccessControlList/PublicRead))

(defn run-watdiv-test
  [{:keys [^crux.api.ICruxSystem crux] :as options} num-tests]
  (let [status (atom nil)
        tests-run (atom 0)
        out-file (io/file (format "watdiv_%s.edn" (System/currentTimeMillis)))]
    (map->WatdivRunner
      {:status status
       :tests-run tests-run
       :out-file out-file
       :num-tests num-tests
       :running-future
       (future
         (try
           (when-not (:done? (.entity (.db crux) ::watdiv-injestion-status))
             (log/info "starting to load watdiv data into crux")
             (reset! status :injesting-watdiv-data)
             (load-rdf-into-crux options "watdiv/data/watdiv.10M.nt")
             (log/info "completed loading watdiv data into crux"))
           (reset! status :running-benchmark)
           (execute-stress-test options tests-run out-file num-tests)
           (reset! status :uploading-results)
           (upload-watdiv-results out-file)
           (reset! status :benchmark-completed)
           (catch Throwable t
             (log/error t "watdiv testrun failed")
             (reset! status :benchmark-failed)
             false)))})))
