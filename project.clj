(defproject core2 "0.1.0-SNAPSHOT"
  :description "Core2 Initiative"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :repositories {"jcenter" {:url "https://jcenter.bintray.com"}}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "0.6.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [org.apache.arrow/arrow-algorithm "3.0.0"]
                 [org.apache.arrow/arrow-vector "3.0.0"]
                 [org.apache.arrow/arrow-memory-netty "3.0.0"]
                 [org.roaringbitmap/RoaringBitmap "0.9.8"]]

  :profiles {:uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [org.clojure/test.check "0.10.0"]
                                  [io.airlift.tpch/tpch "0.10"]
                                  [org.openjdk.jmh/jmh-core "1.27"]
                                  [org.openjdk.jmh/jmh-generator-annprocess "1.27"]
                                  [org.clojure/data.csv "1.0.0"]
                                  [cheshire "5.10.0"]
                                  [org.clojure/tools.namespace "1.1.0"]]
                   :java-source-paths ["src" "jmh"]
                   :resource-paths ["test-resources" "data"]
                   :test-selectors {:default (complement :integration)
                                    :integration :integration}}}

  :aliases {"jmh" ["trampoline" "run"
                   "-m" "org.openjdk.jmh.Main"
                   "-f" "1"
                   "-rf" "json"
                   "-rff" "target/jmh-result.json"]}

  :main core2.core

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
             "--add-modules=ALL-SYSTEM" ;; enables all incubator modules instead
             #_"-Darrow.memory.debug.allocator=true"
             #_"-Darrow.enable_unsafe_memory_access=true"]

  :global-vars {*warn-on-reflection* true})
