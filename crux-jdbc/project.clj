(defproject juxt/crux-jdbc "crux-git-version-beta"
  :description "Crux JDBC"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [seancorfield/next.jdbc "1.1.582"]
                 [com.zaxxer/HikariCP "3.4.5"]
                 [com.taoensso/nippy "2.14.0"]
                 [com.oracle.ojdbc/ojdbc8 "19.3.0.0" :scope "provided"]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
