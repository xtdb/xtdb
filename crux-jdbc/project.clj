(defproject pro.juxt.crux/crux-jdbc "crux-git-version-beta"
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
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

  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn

  :profiles {:dev {:dependencies [[com.opentable.components/otj-pg-embedded "0.13.1"]]}}

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
