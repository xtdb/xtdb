(defproject juxt/crux-jdbc "crux-git-version-beta"
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [seancorfield/next.jdbc "1.1.582"]
                 [org.clojure/java.data "1.0.86"]
                 [com.zaxxer/HikariCP "3.4.5"]
                 [com.taoensso/nippy "2.15.1"]

                 [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]
                 [com.h2database/h2 "1.4.199" :scope "provided"]
                 [com.opentable.components/otj-pg-embedded "0.13.1" :scope "provided"]
                 [org.xerial/sqlite-jdbc "3.28.0" :scope "provided"]
                 [mysql/mysql-connector-java "8.0.17" :scope "provided"]
                 [com.microsoft.sqlserver/mssql-jdbc "8.2.2.jre8" :scope "provided"]]

  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
