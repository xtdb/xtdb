(def core2-version
  (or (System/getenv "CORE2_VERSION")
      "dev-SNAPSHOT"))

(defproject pro.juxt.crux-labs/core2 core2-version
  :description "Core2 Initiative"
  :url "https://github.com/juxt/crux-rnd"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [pro.juxt.clojars-mirrors.com.stuartsierra/dependency "1.0.0"]
                 [org.clojure/data.json]
                 [org.clojure/data.csv]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.apache.arrow/arrow-algorithm "4.0.1"]
                 [org.apache.arrow/arrow-compression "4.0.1"]
                 [org.apache.arrow/arrow-vector "4.0.1"]
                 [org.apache.arrow/arrow-memory-netty "4.0.1"]
                 [org.roaringbitmap/RoaringBitmap "0.9.15"]
                 [pro.juxt.clojars-mirrors.integrant/integrant "0.8.0"]]

  :managed-dependencies [[pro.juxt.crux-labs/core2 ~core2-version]
                         [pro.juxt.crux-labs/core2-datasets ~core2-version]
                         [pro.juxt.crux-labs/core2-kafka ~core2-version]
                         [pro.juxt.crux-labs/core2-s3 ~core2-version]
                         [pro.juxt.crux-labs/core2-jdbc ~core2-version]
                         [pro.juxt.crux-labs/core2-bench ~core2-version]

                         [org.clojure/clojure "1.10.3"]
                         [software.amazon.awssdk/s3 "2.16.76"]
                         [ch.qos.logback/logback-classic "1.2.3"]
                         [org.clojure/data.csv "1.0.0"]
                         [org.clojure/data.json "2.3.1"]]

  :profiles {:dev [:test
                   {:dependencies [[ch.qos.logback/logback-classic]
                                   [org.clojure/tools.namespace "1.1.0"]
                                   [integrant "0.8.0"]
                                   [integrant/repl "0.3.2"]]
                    :repl-options {:init-ns user}
                    :source-paths ["dev"]
                    :resource-paths ["data"]}]

             :with-modules {:dependencies [[pro.juxt.crux-labs/core2-datasets]
                                           [pro.juxt.crux-labs/core2-kafka]
                                           [pro.juxt.crux-labs/core2-s3]
                                           [pro.juxt.crux-labs/core2-jdbc]
                                           [pro.juxt.crux-labs/core2-bench]]}

             :test {:dependencies [[org.clojure/test.check "1.1.0"]
                                   [org.clojure/data.csv "1.0.0"]
                                   [pro.juxt.crux-labs/core2-datasets]

                                   [cheshire "5.10.0"]]

                    :resource-paths ["test-resources"]}

             :jmh {:dependencies [[org.openjdk.jmh/jmh-core "1.32"]
                                  [org.openjdk.jmh/jmh-generator-annprocess "1.32"]]

                   :java-source-paths ["src" "jmh"]}

             :attach-yourkit {:jvm-opts ["-agentpath:/opt/yourkit/bin/linux-x86-64/libyjpagent.so"]}

             :uberjar {:dependencies [[pro.juxt.crux-labs/core2-kafka]
                                      [pro.juxt.crux-labs/core2-s3]
                                      [pro.juxt.crux-labs/core2-jdbc]]
                       :uberjar-name "core2-standalone.jar"
                       :resource-paths ["uberjar"]}}

  :test-selectors {:default (complement (some-fn :skip-test :integration :timescale))
                   :integration :integration
                   :timescale :timescale}

  :aliases {"jmh" ["with-profile" "+jmh"
                   "trampoline" "run"
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

  :global-vars {*warn-on-reflection* true}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace []
                          :omit-source true
                          :filespecs ^:replace []}}

  :repositories {"snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"}}

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :username [:gpg :env/sonatype_username]
                                    :password [:gpg :env/sonatype_password]}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :username [:gpg :env/sonatype_username]
                                     :password [:gpg :env/sonatype_password]}})
