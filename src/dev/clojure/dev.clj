(ns dev
  (:require [clj-async-profiler.core :as clj-async-profiler]
            [clojure.java.browse :as browse]
            [clojure.java.io :as io]
            [integrant.core :as i]
            [integrant.repl :as ir]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.compactor :as c]
            [xtdb.time :as time]
            [xtdb.pgwire :as pgw])
  (:import [java.nio.file Path]
           java.time.Duration
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector.ipc ArrowFileReader]
           (xtdb.trie ArrowHashTrie ArrowHashTrie$IidBranch ArrowHashTrie$Leaf ArrowHashTrie$RecencyBranch ArrowHashTrie$Node)))

(require '[xtdb.logging :refer [set-log-level!]])

(def dev-node-dir
  (io/file "dev/dev-node"))

(def node nil)

(defmethod i/init-key ::xtdb [_ {:keys [node-opts]}]
  (alter-var-root #'node (constantly (xtn/start-node node-opts)))
  node)

(defmethod i/halt-key! ::xtdb [_ node]
  (util/try-close node)
  (alter-var-root #'node (constantly nil)))

(def standalone-config
  {::xtdb {:node-opts {:server {:port 5432
                                :ssl {:keystore (io/file (io/resource "xtdb/pgwire/xtdb.jks"))
                                      :keystore-password "password123"}}
                       :log [:local {:path (io/file dev-node-dir "log")}]
                       :storage [:local {:path (io/file dev-node-dir "objects")}]
                       :metrics [:prometheus {:port 8080}]
                       :http-server {}
                       :flight-sql-server {:port 52358}}}})

(comment
  (do
    (halt)
    (util/delete-dir (util/->path dev-node-dir))
    (go)))


(def playground-config
  {::playground {:port 5439}})

(defmethod i/init-key ::playground [_ {:keys [port]}]
  (pgw/open-playground {:port port}))

(defmethod i/halt-key! ::playground [_ srv]
  (util/close srv))

(ir/set-prep! (fn [] playground-config))
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
                     (tpch/submit-docs! node 0.05))]
        (time (tu/then-await-tx last-tx node (Duration/ofHours 1)))
        (time (tu/finish-chunk! node))
        (time (c/compact-all! node (Duration/ofMinutes 5))))))

  (do
    (newline)
    (doseq [!q [#'tpch/tpch-q1-pricing-summary-report
                #'tpch/tpch-q5-local-supplier-volume
                #'tpch/tpch-q9-product-type-profit-measure]]
      (prn !q)
      (time (tu/query-ra @!q node)))))

(defn read-arrow-file [^Path path]
  (reify clojure.lang.IReduceInit
    (reduce [_ f init]
      (with-open [al (RootAllocator.)
                  ch (util/->file-channel path)
                  rdr (ArrowFileReader. ch al)]
        (.initialize rdr)
        (loop [v init]
          (cond
            (reduced? v) (unreduced v)
            (.loadNextBatch rdr) (recur (f v (vr/rel->rows (vr/<-root (.getVectorSchemaRoot rdr)))))
            :else v))))))

(comment
  (->> (util/->path "/tmp/test.arrow")
       (read-arrow-file)
       (into [] cat)))

(defn read-meta-file
  "Reads the meta file and returns the rendered trie.

     numbers: leaf page idxs
     vectors: iid branches
     maps: recency branches"
  [^Path path]
  (with-open [al (RootAllocator.)
              ch (util/->file-channel path)
              rdr (ArrowFileReader. ch al)]
    (.initialize rdr)
    (.loadNextBatch rdr)

    (letfn [(render-trie [^ArrowHashTrie$Node node]
              (cond
                (instance? ArrowHashTrie$Leaf node) (.getDataPageIndex ^ArrowHashTrie$Leaf node)

                (instance? ArrowHashTrie$RecencyBranch node)
                (let [^ArrowHashTrie$RecencyBranch node node
                      recencies (.getRecencies node)]
                  (into (sorted-map) (zipmap (mapv time/micros->instant recencies)
                                             (mapv (comp render-trie #(.recencyNode node ^long %))
                                                   (range (alength recencies))))))

                (instance? ArrowHashTrie$IidBranch node) (mapv render-trie (.getIidChildren ^ArrowHashTrie$IidBranch node))
                :else node))]

      (render-trie (-> (.getVectorSchemaRoot rdr)
                       (.getVector "nodes")
                       (ArrowHashTrie.)
                       (.getRootNode))))))

(comment
  (->> (util/->path "target/compactor/lose-data-on-compaction/objects/v02/tables/docs/meta/log-l01-nr121-rs16.arrow")
       (read-meta-file)))
