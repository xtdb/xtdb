(defproject com.xtdb/xtdb-grpc-server "<inherited>"
  :description "XTDB HTTP Server"
  
  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}
  :scm {:dir "../.."}

  :java-source-paths
  ["target/generated-sources/protobuf"]
                             
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.xtdb/xtdb-core]
                 [com.google.protobuf/protobuf-java "3.19.3"]
                 [javax.annotation/javax.annotation-api "1.3.2"]
                 [io.netty/netty-codec-http2 "4.1.73.Final"]
                 [io.grpc/grpc-core "1.43.2" :exclusions [io.grpc/grpc-api]]
                 [io.grpc/grpc-netty "1.43.2"
                    :exclusions [io.grpc/grpc-core
                                 io.netty/netty-codec-http2]]
                 [io.grpc/grpc-protobuf "1.43.2"]
                 [io.grpc/grpc-stub "1.43.2"]]
  :main ^:skip-aot xtdb-grpc-server.core
  :target-path "target/%s"
  :profiles {:dev
             {:jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"]
              :dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                                   [com.xtdb/xtdb-test]]}
             :uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
