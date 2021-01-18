(defproject core2 "0.1.0-SNAPSHOT"
  :description "Core2 Initiative"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.6.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [org.apache.arrow/arrow-vector "2.0.0"]
                 [org.apache.arrow/arrow-memory-netty "2.0.0"]
                 [org.roaringbitmap/RoaringBitmap "0.9.3"]]
  :profiles {:uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [org.clojure/test.check "0.10.0"]
                                  [io.airlift.tpch/tpch "0.10"]]}}
  :java-source-paths ["src"]
  :jvm-opts ["-Xmx2G"
             "-XX:MaxDirectMemorySize=2G"
             "-Dio.netty.tryReflectionSetAccessible=true"
             "-Darrow.memory.debug.allocator=true"]
  :global-vars {*warn-on-reflection* true})
