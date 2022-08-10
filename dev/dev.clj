(ns dev
  (:require [clojure.java.io :as io]
            [clojure.java.browse :as browse]
            [core2.ingester :as ingest]
            [core2.node :as node]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util]
            [integrant.core :as i]
            [integrant.repl :as ir]
            [clj-async-profiler.core :as clj-async-profiler])
  (:import [ch.qos.logback.classic Level Logger]
           java.time.Duration
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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
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

(defmethod i/init-key ::core2 [_ {:keys [node-opts]}]
  (alter-var-root #'node (constantly (node/start-node node-opts)))
  node)

(defmethod i/halt-key! ::core2 [_ node]
  (util/try-close node)
  (alter-var-root #'node (constantly nil)))

(def standalone-config
  {::core2 {:node-opts {:core2.log/local-directory-log {:root-path (io/file dev-node-dir "log")}
                        :core2.buffer-pool/buffer-pool {:cache-path (io/file dev-node-dir "buffers")}
                        :core2.object-store/file-system-object-store {:root-path (io/file dev-node-dir "objects")}
                        :core2/server {}
                        :core2/pgwire {:port 5433}
                        :core2.flight-sql/server {:port 52358}}}})

(ir/set-prep! (fn [] standalone-config))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def go ir/go)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def halt ir/halt)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def reset ir/reset)

(def profiler-port 5001)

(defonce profiler-server
  (delay
    (let [port profiler-port
          url (str "http://localhost:" port)]
      (println "Starting serving profiles on" url)
      (clj-async-profiler/serve-files port))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro profile
  "Profiles the given code body with clj-async-profiler, see (browse-profiler) to look at the resulting flamegraph.
  e.g (profile (reduce + (my-function)))
  Options are the same as clj-async-profiler/profile."
  [options? & body]
  `(clj-async-profiler/profile ~options? ~@body))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-profiler
  "Start clj-async-profiler see also: (stop-profiler) (browse-profiler)
  Options are the same as clj-async-profiler/start."
  ([] (clj-async-profiler/start))
  ([options] (clj-async-profiler/start options)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn stop-profiler
  "Stops clj-async-profiler, see (browse-profiler) to go look at the profiles in a nice little UI."
  []
  (let [file (clj-async-profiler/stop)]
    (println "Saved flamegraph to" (str file))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn browse-profiler
  "Opens the clj-async-profiler page in your browser, you can go look at your flamegraphs and start/stop the profiler
  from here."
  []
  @profiler-server
  (browse/browse-url (str "http://localhost:" profiler-port)))

(comment
  #_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
  (def !submit-tpch
    (future
      (let [last-tx (time
                     (tpch/submit-docs! node 0.1))]
        (time (tu/then-await-tx last-tx node (Duration/ofHours 1)))
        (time (tu/finish-chunk node)))))

  (do
    (newline)
    (doseq [!q [#'tpch/tpch-q1-pricing-summary-report
                #'tpch/tpch-q5-local-supplier-volume
                #'tpch/tpch-q9-product-type-profit-measure]]
      (prn !q)
      (let [db (ingest/snapshot (tu/component node :core2/ingester))]
        (time (tu/query-ra @!q db))))))
