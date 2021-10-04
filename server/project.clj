(defproject com.xtdb.labs/core2-server "<inherited>"
  :description "Core2 Server"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir ".."}

  :dependencies [[com.xtdb.labs/core2-api]
                 [com.xtdb.labs/core2-core]

                 [ring/ring-core "1.9.4"]
                 [info.sunng/ring-jetty9-adapter "0.15.2"]
                 [org.eclipse.jetty/jetty-alpn-server "10.0.6"]

                 [metosin/muuntaja "0.6.8"]
                 [metosin/jsonista "0.3.3"]
                 [metosin/reitit-core "0.5.15"]
                 [metosin/reitit-interceptors "0.5.15"]
                 [metosin/reitit-ring "0.5.15"]
                 [metosin/reitit-http "0.5.15"]
                 [metosin/reitit-sieppari "0.5.15"]
                 [metosin/reitit-swagger "0.5.15"]
                 [metosin/reitit-spec "0.5.15"]

                 [com.cognitect/transit-clj]]

  :profiles {:test {:dependencies [[cheshire]]}})
