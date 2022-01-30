(defproject grpc-server "0.0.1-SNAPSHOT"
  :description "XTDB gRPC Server"
  :url "http://example.com/FIXME"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"
            :year 2022
            :key "apache-2.0"}
  :dependencies [[org.clojure/clojure "1.10.3"]
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
  :profiles {:dev {:aliases {"run-dev" ["trampoline" "run" "-m" "grpc-server.server/run-dev"]}
                   :dependencies [[io.pedestal/pedestal.service-tools "0.5.9"]]}
             :uberjar {:aot [grpc-server.server]}}
  :main ^{:skip-aot true} grpc-server.server)


  ;; :parent-project {:path "../../project.clj"
  ;;                  :inherit [:version :repositories :deploy-repositories
  ;;                            :managed-dependencies
  ;;                            :pedantic? :global-vars
  ;;                            :license :url :pom-addition]}
  ;; :scm {:dir "../.."} 