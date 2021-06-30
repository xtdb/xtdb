(defproject pro.juxt.crux/crux-google-cloud-storage "crux-git-version"
  :description "Crux Google Cloud Storage Document Store"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [com.google.cloud/google-cloud-nio "0.122.4"]

                 ;; dep resolution
                 [com.google.api-client/google-api-client "1.31.1"]
                 [com.fasterxml.jackson.core/jackson-core "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.12.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.12.2"]
                 [com.google.guava/guava "30.1.1-jre"]]

  :profiles {:test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]]}}

  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"])
