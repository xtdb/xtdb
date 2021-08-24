(defproject com.xtdb/xtdb-google-cloud-storage "<inherited>"
  :description "XTDB Google Cloud Storage Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.xtdb/xtdb-core]
                 [com.google.cloud/google-cloud-nio "0.122.4"]

                 ;; dep resolution
                 [com.google.api-client/google-api-client "1.31.1"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [com.google.guava/guava "30.1.1-jre"]]

  :profiles {:test {:dependencies [[com.xtdb/xtdb-test]]}}

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"])
