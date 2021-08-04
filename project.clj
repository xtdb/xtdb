(def core2-version
  (or (System/getenv "CORE2_VERSION")
      "dev-SNAPSHOT"))

(defproject pro.juxt.crux-labs/core2-dev core2-version
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :managed-dependencies [[pro.juxt.crux-labs/core2-api ~core2-version]
                         [pro.juxt.crux-labs/core2-core ~core2-version]
                         [pro.juxt.crux-labs/core2-server ~core2-version]
                         [pro.juxt.crux-labs/core2-client ~core2-version]
                         [pro.juxt.crux-labs/core2-datasets ~core2-version]
                         [pro.juxt.crux-labs/core2-kafka ~core2-version]
                         [pro.juxt.crux-labs/core2-s3 ~core2-version]
                         [pro.juxt.crux-labs/core2-jdbc ~core2-version]
                         [pro.juxt.crux-labs/core2-bench ~core2-version]

                         [org.clojure/clojure "1.10.3"]
                         [software.amazon.awssdk/s3 "2.16.76"]
                         [ch.qos.logback/logback-classic "1.2.3"]
                         [org.clojure/data.csv "1.0.0"]
                         [org.clojure/data.json "2.3.1"]
                         [com.cognitect/transit-clj "1.0.324"]

                         [cheshire "5.10.0"]
                         [hato "0.8.2"]]

  :profiles {:dev [:test
                   {:dependencies [[pro.juxt.crux-labs/core2-api]
                                   [pro.juxt.crux-labs/core2-core]
                                   [pro.juxt.crux-labs/core2-server]
                                   [pro.juxt.crux-labs/core2-client]
                                   [pro.juxt.crux-labs/core2-datasets]
                                   [pro.juxt.crux-labs/core2-kafka]
                                   [pro.juxt.crux-labs/core2-s3]
                                   [pro.juxt.crux-labs/core2-jdbc]
                                   [pro.juxt.crux-labs/core2-bench]

                                   [ch.qos.logback/logback-classic]
                                   [org.clojure/tools.namespace "1.1.0"]
                                   [integrant "0.8.0"]
                                   [integrant/repl "0.3.2"]]
                    :repl-options {:init-ns user}
                    :source-paths ["dev"]
                    :resource-paths ["data"]}]

             :test {:dependencies [[org.clojure/test.check "1.1.0"]
                                   [org.clojure/data.csv "1.0.0"]
                                   [pro.juxt.crux-labs/core2-datasets]

                                   [cheshire "5.10.0"]
                                   [hato "0.8.2"]]
                    :resource-paths ["test-resources"]}

             :jmh {:dependencies [[org.openjdk.jmh/jmh-core "1.32"]
                                  [org.openjdk.jmh/jmh-generator-annprocess "1.32"]]

                   :java-source-paths ["src" "jmh"]}

             :attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}

             :uberjar {:dependencies [[pro.juxt.crux-labs/core2-core]
                                      [pro.juxt.crux-labs/core2-server]
                                      [pro.juxt.crux-labs/core2-client]
                                      [pro.juxt.crux-labs/core2-kafka]
                                      [pro.juxt.crux-labs/core2-s3]
                                      [pro.juxt.crux-labs/core2-jdbc]]
                       :uberjar-name "core2-standalone.jar"
                       :resource-paths ["uberjar"]
                       :main ^:skip-aot core2.main}

             :no-asserts {:jvm-opts ["-da" "-Dclojure.spec.check-asserts=false"]}}

  :aliases {"jmh" ["with-profile" "+jmh"
                   "trampoline" "run"
                   "-m" "org.openjdk.jmh.Main"
                   "-f" "1"
                   "-rf" "json"
                   "-rff" "target/jmh-result.json"]}

  :jvm-opts ["-Xmx2G"
             "-XX:MaxDirectMemorySize=2G"
             "-Dio.netty.tryReflectionSetAccessible=true"
             "-Darrow.enable_null_check_for_get=false"
             "-Dclojure.spec.check-asserts=true"
             "--illegal-access=warn" ;; needed on JDK16 to allow Netty/Arrow access DirectBuffer internals
             #_"--add-modules=jdk.incubator.vector" ;; doesn't work if it doesn't exist, like on JDK11.
             #_"--add-modules=ALL-SYSTEM" ;; enables all incubator modules instead
             #_"-Darrow.memory.debug.allocator=true"
             #_"-Darrow.enable_unsafe_memory_access=true"]

  :global-vars {*warn-on-reflection* true}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :repositories {"snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"}}

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :username [:gpg :env/sonatype_username]
                                    :password [:gpg :env/sonatype_password]}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :username [:gpg :env/sonatype_username]
                                     :password [:gpg :env/sonatype_password]}})
