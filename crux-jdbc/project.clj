(defproject juxt/crux-jdbc "derived-from-git"
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "0.5.0"]
                 [juxt/crux-core "derived-from-git"]
                 [seancorfield/next.jdbc "1.0.9"]
                 [org.clojure/java.data "0.1.4"]
                 [com.zaxxer/HikariCP "3.3.1"]
                 [com.taoensso/nippy "2.14.0"]]
  :middleware [leiningen.project-version/middleware])
