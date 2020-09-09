(defproject juxt/soak "crux-git-version"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "1.0.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-kafka "crux-git-version-beta"]
                 [juxt/crux-rocksdb "crux-git-version-beta"]
                 [juxt/crux-metrics "crux-git-version-alpha"]

                 [bidi "2.1.6"]
                 [hiccup "2.0.0-alpha2"]
                 [cheshire "5.10.0"]
                 [clj-http "3.10.0"]
                 [ring "1.8.0"]

                 [software.amazon.awssdk/secretsmanager "2.10.91"]
                 [jarohen/nomad "0.9.0"]

                 [integrant "0.8.0"]
                 [integrant/repl "0.3.1"]

                 [org.slf4j/slf4j-simple "1.7.26"]

                 ;; deps res
                 [org.eclipse.jetty/jetty-util "9.4.22.v20191022"]
                 [org.apache.httpcomponents/httpclient "4.5.12"]]

  :middleware [leiningen.project-version/middleware]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-soak.jar"
  :pedantic? :warn)
