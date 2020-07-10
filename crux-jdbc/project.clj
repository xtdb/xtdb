(defproject juxt/crux-jdbc "crux-git-version-beta"
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.0.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [seancorfield/next.jdbc "1.0.9"]
                 [org.clojure/java.data "0.1.4"]
                 [com.zaxxer/HikariCP "3.3.1"]
                 [com.taoensso/nippy "2.14.0"]
                 [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
