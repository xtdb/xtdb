(defproject juxt/soak "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.7"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-rocksdb "derived-from-git"]
                 [juxt/crux-metrics "derived-from-git"]

                 [clj-http "3.10.0"]
                 [ring "1.8.0"]
                 [software.amazon.awssdk/s3 "2.10.61"]
                 ;; deps res
                 [org.eclipse.jetty/jetty-util "9.4.22.v20191022"]
                 [org.apache.httpcomponents/httpclient "4.5.9"]]

  :middleware [leiningen.project-version/middleware]
  :jvm-opts ["-Xms3g" "-Xmx3g"]
  :uberjar-name "crux-soak.jar"
  :pedantic? :warn)
