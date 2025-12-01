#!/usr/bin/env bb

(require '[babashka.cli :as cli]
         '[babashka.process :as p]
         '[clojure.tools.logging :as log]
         '[taoensso.timbre :as timbre])

(timbre/set-level! :debug)

(def cli-opts {:coerce {:load-phase :boolean :duration :string}})
(def opts (cli/parse-opts *command-line-args* cli-opts))

(log/debug "Given options: " opts)

(def c1-auctionmark-file "/tmp/auctionmark-c1-run.edn")
(def c2-auctionmark-file "/tmp/auctionmark-c2-run.edn")
(def jvm-args ["-Xmx2g", "-Xms2g", "-XX:MaxDirectMemorySize=3g", "-XX:MaxMetaspaceSize=1g"])

(log/info "Checking out C1 auctionmark branch")
(p/shell {:dir "../../../xtdb/"} "git checkout auctionmark")

(log/info "Running auctionmark on C1")
(log/debug "C1 process result:"
           @(p/process {:inherit true
                        :shutdown p/destroy-tree
                        :extra-env {"MALLOC_ARENA_MAX" "2"
                                    "JVM_OPTS" (->> jvm-args (interpose " ") (apply str))}
                        :dir "../../../xtdb/bench/"}
                       (cond-> (str "lein run -m xtdb.bench2.core1 --output-file " c1-auctionmark-file " ")
                         (not (nil? (:load-phase opts))) (str "--load-phase " (:load-phase opts) " ")
                         (:duration opts) (str "--duration " (:duration opts)))))

(log/info "Running auctionmark on C2")
(let [args (cond-> (str "-Poutput-file=" c2-auctionmark-file " ")
             (not (nil? (:load-phase opts))) (str "-Pload-phase=" (:load-phase opts) " ")
             (:duration opts) (str "-Pduration=" (:duration opts))
             true (str "'"))]

  (log/debug "C2 process result:"
             @(p/process {:inherit true
                          :shutdown p/destroy-tree
                          :dir "../../"}
                         (str "./gradlew run-auctionmark " args))))

(log/info "Generating comparison report")
(log/debug "Generating reports process result:"
           @(p/process {:inherit true
                        :shutdown p/destroy-tree
                        :dir "../../"}
                       (str "./gradlew create-reports "
                            "-Preport0=[core1," c1-auctionmark-file "] "
                            "-Preport1=[core2," c2-auctionmark-file "]")))
