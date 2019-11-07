(defproject juxt/crux-generate-api "derived-from-git"
  :description "Crux Generate Api"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]
                 [com.squareup/javapoet "1.9.0"]]
  :middleware [leiningen.project-version/middleware]
  :aliases {"generate"
            ["do"
             ["run" "-m" "crux.gen-topology-classes/gen-topology-file" "KafkaTopology" "crux.kafka/topology"]
             ["run" "-m" "crux.gen-topology-classes/gen-topology-file" "JDBCTopology" "crux.jdbc/topology"]
             ["run" "-m" "crux.gen-topology-classes/gen-topology-file" "StandaloneTopology" "crux.standalone/topology"]]}
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
