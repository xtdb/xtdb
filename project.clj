(defproject core2 "0.1.0-SNAPSHOT"
  :description "Core2 Initiative"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "0.6.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [com.stuartsierra/dependency "0.2.0"]
                 [org.clojure/data.json "2.0.2"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.apache.arrow/arrow-algorithm "4.0.0"]
                 [org.apache.arrow/arrow-compression "4.0.0"]
                 [org.apache.arrow/arrow-vector "4.0.0"]
                 [org.apache.arrow/arrow-memory-netty "4.0.0"]
                 [org.roaringbitmap/RoaringBitmap "0.9.10"]]

  :profiles {:dev [:datasets :s3 :kafka
                   {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                   [org.clojure/tools.namespace "1.1.0"]
                                   [integrant "0.8.0"]
                                   [integrant/repl "0.3.1"]

                                   [org.clojure/test.check "0.10.0"]
                                   [org.clojure/data.csv "1.0.0"]
                                   [org.openjdk.jmh/jmh-core "1.27"]
                                   [org.openjdk.jmh/jmh-generator-annprocess "1.27"]
                                   [cheshire "5.10.0"]]
                    :repl-options {:init-ns user}
                    :source-paths ["dev"]
                    :java-source-paths ["src" "jmh"]
                    :resource-paths ["test-resources" "data"]
                    :test-selectors {:default (complement (some-fn :integration :kafka :timescale))
                                     :integration :integration
                                     :kafka :kafka
                                     :timescale :timescale}}]

             ;; TODO debate best way to multi-module this
             ;; for now, I just want to ensure they're sufficiently isolated
             :datasets {:source-paths ["modules/datasets/src"]
                        :dependencies [[io.airlift.tpch/tpch "0.10"]
                                       [org.clojure/data.csv "1.0.0"]
                                       [software.amazon.awssdk/s3 "2.10.91"]]}

             :s3 {:source-paths ["modules/s3/src"]
                  :java-source-paths ["modules/s3/src"]
                  :test-paths ["modules/s3/test"]
                  :dependencies [[software.amazon.awssdk/s3 "2.10.91"]]}

             :kafka {:source-paths ["modules/kafka/src"]
                     :test-paths ["modules/kafka/test"]
                     :dependencies [[org.apache.kafka/kafka-clients "2.7.0"]]}

             :bench [:s3 :kafka :datasets
                     {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                      :resource-paths ["modules/bench/resources"]
                      :source-paths ["modules/bench/src"]
                      :test-paths ["modules/bench/test"]
                      :main ^:skip-aot clojure.main
                      :uberjar-name "core2-bench.jar"}]

             :attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}}

  :aliases {"jmh" ["trampoline" "run"
                   "-m" "org.openjdk.jmh.Main"
                   "-f" "1"
                   "-rf" "json"
                   "-rff" "target/jmh-result.json"]}

  :main core2.main
  :aot [core2.main]

  :java-source-paths ["src"]

  :javac-options ["-source" "11" "-target" "11"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :jvm-opts ["-Xmx2G"
             "-XX:MaxDirectMemorySize=2G"
             "-Dio.netty.tryReflectionSetAccessible=true"
             "-Darrow.enable_null_check_for_get=false"
             "--illegal-access=warn" ;; needed on JDK16 to allow Netty/Arrow access DirectBuffer internals
             #_"--add-modules=jdk.incubator.vector" ;; doesn't work if it doesn't exist, like on JDK11.
             #_"--add-modules=ALL-SYSTEM" ;; enables all incubator modules instead
             #_"-Darrow.memory.debug.allocator=true"
             #_"-Darrow.enable_unsafe_memory_access=true"]

  :global-vars {*warn-on-reflection* true})
