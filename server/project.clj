(defproject pro.juxt.crux-labs/core2-server "<inherited>"
  :description "Core2 Server"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[pro.juxt.crux-labs/core2-api]
                 [pro.juxt.crux-labs/core2-core]
                 #_[pro.juxt.clojars-mirrors.crux/crux-http-server-deps "0.0.2"]

                 [ring/ring-core "1.9.4"]
                 [info.sunng/ring-jetty9-adapter "0.14.2"]
                 [org.eclipse.jetty/jetty-alpn-openjdk8-server "9.4.36.v20210114"]

                 [metosin/muuntaja "0.6.8"]
                 [metosin/jsonista "0.3.1"]
                 [metosin/reitit-core "0.5.12"]
                 [metosin/reitit-interceptors "0.5.12"]
                 [metosin/reitit-ring "0.5.12"]
                 [metosin/reitit-http "0.5.12"]
                 [metosin/reitit-sieppari "0.5.13"]
                 [metosin/reitit-swagger "0.5.12"]
                 [metosin/reitit-spec "0.5.12"]

                 [com.cognitect/transit-clj]]

  :profiles {:test {:dependencies [[cheshire]
                                   [hato]]}})
