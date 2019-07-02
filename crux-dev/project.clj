(defproject juxt/crux-dev :derived-from-git
  :description "Crux Dev Dependencies"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/test.check "0.10.0-alpha3"]
                 [ch.qos.logback/logback-classic "1.2.3"]]
  :middleware [leiningen.project-version/middleware])
