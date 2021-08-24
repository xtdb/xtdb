(defproject com.xtdb/xtdb-jdbc "<inherited>"
  :description "XTDB JDBC"

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
                 [pro.juxt.clojars-mirrors.com.github.seancorfield/next.jdbc "1.2.674"]
                 [org.clojure/java.data "1.0.86"]
                 [com.zaxxer/HikariCP "3.4.5"]
                 [pro.juxt.clojars-mirrors.com.taoensso/nippy "3.1.1"]

                 ;; Sample driver dependencies
                 [org.postgresql/postgresql "42.2.18" :scope "provided"]
                 [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]
                 [com.h2database/h2 "1.4.200" :scope "provided"]
                 [org.xerial/sqlite-jdbc "3.28.0" :scope "provided"]
                 [mysql/mysql-connector-java "8.0.23" :scope "provided"]
                 [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8" :scope "provided"]]

  :profiles {:dev {:dependencies [[com.opentable.components/otj-pg-embedded "0.13.1"]]}})
