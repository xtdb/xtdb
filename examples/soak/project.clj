(defproject juxt/soak "<inherited>"
  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.json "1.0.0"]
                 [pro.juxt.crux/crux-core]
                 [pro.juxt.crux/crux-kafka]
                 [pro.juxt.crux/crux-rocksdb]
                 [pro.juxt.crux/crux-metrics]
                 [pro.juxt.crux/crux-s3]

                 [bidi "2.1.6"]
                 [hiccup "2.0.0-alpha2"]
                 [cheshire "5.10.0"]
                 [clj-http "3.10.1"]
                 [ring "1.8.1"]

                 [software.amazon.awssdk/secretsmanager "2.10.91"]
                 [jarohen/nomad "0.9.0"]

                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]

                 [org.slf4j/slf4j-simple "1.7.26"]

                 ;; deps res
                 [org.eclipse.jetty/jetty-util "9.4.36.v20210114"]
                 [org.apache.httpcomponents/httpclient "4.5.13"]]

  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-soak.jar"
  :pedantic? :warn)
