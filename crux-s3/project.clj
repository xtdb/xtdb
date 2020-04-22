(defproject juxt/crux-s3 "derived-from-git"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.0.0"]

                 [juxt/crux-core "derived-from-git"]
                 [software.amazon.awssdk/s3 "2.10.61"]

                 ;; dependency resolution
                 [io.netty/netty-codec "4.1.45.Final"]
                 [io.netty/netty-codec-http "4.1.45.Final"]
                 [io.netty/netty-handler "4.1.45.Final"]
                 [com.fasterxml.jackson.core/jackson-core "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.10.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.10.2"]
                 [org.reactivestreams/reactive-streams "1.0.3"]]

  :middleware [leiningen.project-version/middleware]

  :pedantic? :warn)
