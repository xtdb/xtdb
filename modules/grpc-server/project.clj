(defproject com.xtdb/xtdb-grpc-server "<inherited>"
  :description "XTDB gRPC Server"
  :plugins [[lein-parent "0.3.8"]
            [lein-cljfmt "0.8.0"]]
  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}
  :scm {:dir "../.."} 

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [com.xtdb/xtdb-core "1.20.0"]
                 [io.pedestal/pedestal.service "0.5.9"]

                 ;; -- PROTOC-GEN-CLOJURE --
                 [io.github.protojure/grpc-server "2.0.1"]
                 [io.github.protojure/google.protobuf "2.0.0"]

                 [ch.qos.logback/logback-classic "1.2.9"]
                 [org.slf4j/jul-to-slf4j "1.7.32"]
                 [org.slf4j/jcl-over-slf4j "1.7.32"]
                 [org.slf4j/log4j-over-slf4j "1.7.32"]]
  :min-lein-version "2.0.0"
  :resource-paths ["config", "resources"]
  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev {:aliases {"run-dev" ["run" "-m" "xtdb.grpc-server.server/run-dev"]}
                   :dependencies [[io.pedestal/pedestal.service-tools "0.5.9"]]}
             :test {:dependencies [[io.pedestal/pedestal.service-tools "0.5.9"]
                                   [com.xtdb/xtdb-test]]}
             :uberjar {:aot [xtdb.grpc-server.server]}}
  :main ^{:skip-aot true} xtdb.grpc-server.server)
