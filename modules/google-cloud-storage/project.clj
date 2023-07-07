(defproject com.xtdb/xtdb-google-cloud-storage "<inherited>"
  :description "XTDB Google Cloud Storage Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [com.xtdb/xtdb-core]
                 [com.google.cloud/google-cloud-nio "0.124.10"]
                 [com.google.cloud/google-cloud-storage "2.23.0"]
                 [com.google.guava/guava "31.0.1-jre"]]

  :profiles {:test {:dependencies [[com.xtdb/xtdb-test]]}}

  :jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"])
